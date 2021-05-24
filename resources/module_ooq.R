# Optimize the following LP problem:
# - How much to buy given a demand forecast.
# - How to split the buying suggestion in multiple orders.
# - How to time these orders for each planning market.

# TODO: Add to environment variables
DAYS_IN_WEEK = 7
QUANTIFICATION_LIFESPAN_TOLERANCE = 5
FALLBACK_LIFESPAN_DEMAND_MULTIPLIER = 1.3
# Percentage of the initial safety stock for relaxing the minimum trail constraint
MINIMUM_TRAIL_ACTIVATION_MULTIPLIER = 0.1

# We use a timeout value here so Gurobi cannot hold the R-agent hostage for a
# prolonged period if it for some reason gets an extremely difficult problem
GUROBI_TIMEOUT_VALUE <- 60

# Function that takes an instance of AsqJob and generate buying suggestions for the target season
process_job_ooq = function(asq_job) {
  flog.info(
    "Execute OOQ.",
    scope = "OOQ",
    job_id = asq_job$job_id
  )
  TIMING$start("ooq", asq_job$job_id)

  # The OOQ module only provides orders for target season
  target_season = max(asq_job$planning_market_features$season_id)
  target_pas_code = (asq_job$article_features %>% filter(season_id == target_season))$pas_code

  # Build the arguments to the run function
  weekly_demand = asq_job$get_seasonless_data() %>% filter(main_pas_code == target_pas_code)
  argument_weekly_demand_forecast = tibble(
    pm = weekly_demand$planning_market_id,
    article_id = target_pas_code,
    week_life = weekly_demand$week_life,
    week_demand = weekly_demand$forecasted_demand,
    historical_cumulative_error = weekly_demand$historical_cumulative_error_on_demand
  )

  data = asq_job$get_lifespan_demand_by_planning_market() %>%
    left_join(asq_job$get_forecasted_number_of_stores_by_planning_market(), by = c('planning_market_id', 'pas_code')) %>%
    filter(pas_code == target_pas_code)

  r3a_arg = tibble(
    pm = data$planning_market_id,
    fc_lifespan_demand = data$lifespan_demand,
    nb_of_stores_predicted = data$forecasted_store_number,
    article_id = target_pas_code
  )

  target_planning_market_features = asq_job$planning_market_features %>% filter(pas_code == target_pas_code)
  target_article = asq_job$article_features %>% filter(pas_code == target_pas_code)
  features_arg = tibble(
    pm = target_planning_market_features$planning_market_id,
    article_id = target_planning_market_features$pas_code,
    in_shop_week_date = get_hm_date_from_week_name(target_planning_market_features$in_shop_week),
    lifespan_days = DAYS_IN_WEEK * coalesce(target_planning_market_features$lifespan, target_planning_market_features$lifespan_ri),
    lead_time_week = get_hm_date_from_week_name(target_planning_market_features$lead_time_week),
    concept = target_article$subindex_code,
    department_name = target_article$department_name,
    core_extended = target_planning_market_features$core_extended_flag,
    ri_flag = target_planning_market_features$is_ri_user, # What matters here is the user opinion, not the forecast method
    nb_of_sizes = target_planning_market_features$nb_of_sizes,
    iaq_one_store = target_planning_market_features$iaq_one_store
  )

  # Adding pm features into ingoing stock.
  ingoing_stock_arg <- asq_job$get_seasonless_data() %>%
    left_join(
      asq_job$planning_market_features %>%
        filter(season_id == target_season) %>%
        select(planning_market_id, in_shop_week, lead_time_week),
      by = c("planning_market_id")
    )

  # TODO: Remove the if statement which is part of the technical debt added by the feature flag and enable a smooth transition of def order feature.
  # Get the last week historic stock from `ingoing_stock_arg`.
  if (asq_job$feature_flags[["IS_DEF_ORDER_ACTIVE"]]) {
    # Find which pm has no ancestor until max between lead time or in shop week date.
    has_no_ancestors_with_stock <- ingoing_stock_arg %>%
      filter(week_name < pmax(in_shop_week, lead_time_week)) %>%
      group_by(planning_market_id) %>%
      summarize(have_no_stock_history = all(is.na(forecasted_outgoing_stock)))

    ingoing_stock_arg <- ingoing_stock_arg %>%
      filter(week_name == pmax(in_shop_week, lead_time_week))

  } else {

    # Find which pm has no ancestor.
    has_no_ancestors_with_stock <- ingoing_stock_arg %>%
      filter(week_name < in_shop_week) %>%
      group_by(planning_market_id) %>%
      summarize(have_no_stock_history = all(is.na(forecasted_outgoing_stock)))

    ingoing_stock_arg <- ingoing_stock_arg %>%
      filter(week_name == in_shop_week)

  }

  # Merge the flag about having historic stock.
  ingoing_stock_arg <- ingoing_stock_arg %>% left_join(has_no_ancestors_with_stock,
                                                       by = c('planning_market_id'))

  if (any(ingoing_stock_arg$have_no_stock_history)) {
    flog.info(
      paste(
        "Filling pm with no historic stock:",
        paste(
          ingoing_stock_arg[ingoing_stock_arg$have_no_stock_history == TRUE, ]$planning_market_id,
          collapse=" "
        )
      ),
      scope = "OOQ",
      job_id = asq_job$job_id
    )

    # Fill pm with no stock history with 0.
    ingoing_stock_arg[ingoing_stock_arg$have_no_stock_history == TRUE,]$forecasted_ingoing_stock = 0
  }

  # Format ingoing stock table.
  ingoing_stock_arg <- ingoing_stock_arg %>%
    left_join(
      asq_job$get_orders() %>%
        filter(order_type == "def") %>%
        select(planning_market_id, pas_code, week_name, order_type, order_size),
      by = c("planning_market_id", "main_pas_code" = "pas_code", "week_name")
    ) %>%
    mutate(article_id = target_pas_code) %>%
    rename(pm = planning_market_id, ingoing_stock = forecasted_ingoing_stock) %>%
    select(pm, article_id, ingoing_stock)

  # Validate that no recurring planning markets with NA as ingoing stock for target season exist.
  if (anyNA(ingoing_stock_arg$ingoing_stock)) {
    # Find problematic planning markets.
    planning_markets_with_na_ingoing_stock <-
      ingoing_stock_arg %>%
      filter(is.na(ingoing_stock))

    # Format as string.
    planning_markets_with_na_ingoing_stock <- toString(
      planning_markets_with_na_ingoing_stock$pm %>%
      sort()
    )

    # Stop quantification, the root cause for an NA ingoing stock needs to be investigated.
    stop(
      sprintf(
        "OOQ did not find ingoing stock for recurring planning markets: %s.",
        planning_markets_with_na_ingoing_stock
        )
      )
  }

  # Getting every orders from asq_job
  orders <- asq_job$get_orders() %>%
                mutate(week_name = get_hm_date_from_week_name(week_name)) %>%
                rename(article_id = pas_code,
                       pm = planning_market_id,
                       order_in_shop_week_date = week_name,
                       quantity = order_size
                       )

  # Filter def orders
  def_orders <- orders %>% filter(order_type == "def")

  # Filter sim orders
  sim_orders <- orders %>% filter(order_type == "sim")

  # TODO: Remove to enable to have sim orders and def orders on the same week.
  # Having sim and def orders on the same week make the quantity ordered increasable.
  sim_orders <- sim_orders[0,]

  # Check if use lead time is needed.
  # Def order is only necessary if there is def orders or if there is one pm lead
  # time week that is after the in shop week and that will block ooq to behave as usual.
  IS_DEF_ORDER_ACTIVE <- asq_job$feature_flags[["IS_DEF_ORDER_ACTIVE"]] &
                          ((nrow(def_orders) != 0) | any(features_arg$lead_time_week > features_arg$in_shop_week_date))

  if (IS_DEF_ORDER_ACTIVE) {
    flog.info(
      "Def Order and Lead Time logic active!",
      scope="OOQ",
      job_id = asq_job$job_id
    )
  }

  flog.info(
    "Pipeline calling the OOQ module...",
    scope="OOQ",
    job_id = asq_job$job_id
  )
  res <- run_ooq(
    r3a_output_tl = r3a_arg,
    art_features_tl = features_arg,
    wdf_output_tl = argument_weekly_demand_forecast,
    ingoing_stock_t = ingoing_stock_arg,
    def_orders = def_orders,
    sim_orders = sim_orders,
    is_def_order_active = IS_DEF_ORDER_ACTIVE,
    window_end = max(asq_job$window$end, max(get_hm_date_from_week_name(asq_job$planning_market_features$in_shop_week) + coalesce(asq_job$planning_market_features$lifespan,
                                                                                                                     asq_job$planning_market_features$lifespan_ri),
                                             na.rm = T)), # Either the end of the UI window, or latest end of lifespan
    ooq_parameters = asq_job$resources$ooq_params,
    asq_job$job_id
  )

  # Dump OOQ inputs to Redis, to expose as traces in Cockpit.
  TRACE$dump(
    step = "orders_before_OOQ",
    table = asq_job$get_orders(),
    job_id = asq_job$job_id
  )
  TRACE$dump(
    step = "OOQ_planning_market_level_parameters",
    table = list(res$orders$pm_level_params),
    job_id = asq_job$job_id
  )
  TRACE$dump(
    step = "OOQ_week_level_parameters",
    table = list(res$orders$week_level_params),
    job_id = asq_job$job_id
  )
  if (any(res$orders$data$ooq < OOQ_MINIMUM_ORDER_QUANTITY)){
    stop("OOQ has suggested orders of size less than the minimum acceptable value")
  }
  # Check if there is suggested orders
  if (nrow(res$orders$data)){
    # OOQ will `re-suggest` def orders in order to be taken into account in the optimizer.
    # We need to take those def orders suggestion out before giving our suggestion.
    res$orders$data <- anti_join(res$orders$data, def_orders, by = c('pm', 'article_id', 'order_in_shop_week_date'))

    # Special treatments for orders that may fall on week 53. Change their date so that they actually fall on W1 of the next year
    res$orders$data = res$orders$data %>% mutate(week_name = get_hm_week_name(order_in_shop_week_date),
                                                week_name = if_else(substr(week_name, 5, 6) == 53,
                                                                    paste0(as.integer(substr(week_name, 1, 4)) + 1, "01"),
                                                                    week_name)) %>%
      select(-order_in_shop_week_date)

    flog.info(
      "OOQ module executed, updating asq_job.",
      scope="OOQ",
      job_id = asq_job$job_id
    )

    TRACE$dump(
      step = paste0("OOQ_s", target_season),
      table =
        res$orders$data %>%
        arrange(pm),
      job_id = asq_job$job_id
    )

    # Append the OOQ suggestion with a type designation "ooq"
    asq_job$append_orders(
      planning_market_ids = res$orders$data$pm,
      pas_codes = rep(target_pas_code, length(res$orders$data$pm)),
      week_names = res$orders$data$week_name,
      order_types = "ooq", # These orders are necessarily of type "OOQ"
      order_numbers = res$orders$data$order_number,
      order_sizes = res$orders$data$quantity,
      order_optimal_quantities = res$orders$data$ooq,
      week_life = res$orders$data$week_life
    )

    # Update the unified log with all information. We need to gather information from various places
    arg1 = ingoing_stock_arg %>% select(pm, ingoing_stock)
    arg2 = res$orders$data %>% group_by(pm) %>% mutate(nb_of_orders = n()) %>% ungroup() %>% select(pm, ooq, nb_of_orders) %>% rename(optimal_order_quantity = ooq)
    arg3 = res$orders$pm_level_params %>% select(pm, min_ground_qty, min_trail_qty, iaq, all_store_quantity, tmq_percent)
    arg4 = res$orders$week_level_params %>% as_tibble %>% filter(week_life == 1) %>% select(pm, safety_stock) %>% rename(start_safety_stock = safety_stock)

    to_log = arg1 %>% left_join(arg2, by = c('pm')) %>%
      left_join(arg3, by = c('pm')) %>%
      left_join(arg4, by = c('pm')) %>%
      mutate(planning_market_id = pm)

    asq_job$update_unified_log(to_log)

    TIMING$end("ooq", asq_job$job_id)

  } else {
    flog.info(
      "OOQ has suggested zero orders!",
      scope="OOQ",
      job_id = asq_job$job_id
    )
  }
}


# Main entry point for the OOQ logic
run_ooq = function(r3a_output_tl,
                   art_features_tl,
                   wdf_output_tl,
                   ingoing_stock_tl,
                   def_orders,
                   sim_orders,
                   is_def_order_active,
                   window_end,
                   ooq_parameters,
                   job_id = -1) {
  input_table = r3a_output_tl %>%
    left_join(ingoing_stock_tl, by = c("article_id", "pm"))
  input_table = mutate(input_table,
                       ingoing_stock = coalesce(ingoing_stock, 0))

  input_table = input_table %>%
    left_join(art_features_tl, by = c("article_id", "pm"))

  input_table = input_table %>%
    left_join(ooq_parameters, by = c(pm = "planning_market_id"))

  wdf_output_tl = wdf_output_tl %>% arrange(week_life)

  ## results will be one table with one row per article containing the orders split in qty and timing
  articles = data.frame(unique(input_table[, c("article_id",
                                               "pm",
                                               "lifespan_days",
                                               "fc_lifespan_demand",
                                               "nb_of_stores_predicted",
                                               "ingoing_stock",
                                               "in_shop_week_date",
                                               "lead_time_week",
                                               "region_id",
                                               "nb_of_sizes",
                                               "iaq_one_store",
                                               "core_extended",
                                               "tmq_percent",
                                               "ri_flag",
                                               "weeks_of_coverage",
                                               "min_trail_size_pct",
                                               "min_ground_size_pct",
                                               "last_trail_timing",
                                               "qos_target",
                                               "max_number_of_sizes",
                                               "max_EOP",
                                               "max_number_of_trails",
                                               "min_split_timing",
                                               "max_s_percent",
                                               "M",
                                               "safety_factor",
                                               "reg_trail_size")]))

  articles = mutate(articles,
                    nb_of_sizes = pmax(nb_of_sizes, 3))

  if (is_def_order_active) {

    # Filter articles that do not need quantification
    # because the lead time week is later than the last trail date,
    # therefore we can not suggest any order for those articles.
    applicable_articles <- (articles$in_shop_week_date
                            + articles$lifespan_days
                            - articles$last_trail_timing * DAYS_IN_WEEK
                            > articles$lead_time_week)

    articles <- articles[applicable_articles,]
    }

  result = tibble()
  pm_level_params = tibble()
  week_level_params = tibble()

  iterator = articles %>% select(article_id, region_id) %>% unique()

  if (nrow(iterator)){
    for (i in 1:nrow(iterator)) {
      relevant = articles %>%
        filter(article_id == iterator[i, "article_id"] & region_id == iterator[i, "region_id"]) %>%
        arrange(-fc_lifespan_demand)

      start_date = min(relevant$in_shop_week_date)
      n_countries = nrow(relevant)
      lead_time = relevant$lead_time_week
      relevant$end_date = relevant$in_shop_week_date + relevant$lifespan_days
      total_lifespan_w = as.integer((max(relevant$end_date) - start_date) / DAYS_IN_WEEK)
      relevant$week_to_start = 1 + as.integer((relevant$in_shop_week_date - start_date) / DAYS_IN_WEEK)
      lead_time_weeks = if_else(rep(is_def_order_active, n_countries),
                                      pmax(0, as.integer((lead_time - relevant$in_shop_week_date) / DAYS_IN_WEEK)),
                                      rep(0, n_countries))
      last_week_delay = max(relevant$week_to_start)

      article_id = relevant$article_id
      factor = relevant$factor
      prod_name = relevant$prod_name
      colour_name = relevant$colour_name
      pm = relevant$pm
      bq = relevant$bq
      fc_lifespan_demand = relevant$fc_lifespan_demand
      core_extended = relevant$core_extended
      iaq_one_store = relevant$iaq_one_store
      nb_of_stores_predicted = pmin(relevant$nb_of_stores_predicted, relevant$fc_lifespan_demand / iaq_one_store)
      iaq = iaq_one_store * nb_of_stores_predicted
      week_to_start = relevant$week_to_start
      ri_flag = relevant$ri_flag
      ingoing_stock = relevant$ingoing_stock
      tmq_percent = relevant$tmq_percent
      in_shop_week_date = relevant$in_shop_week_date
      nb_of_sizes = if_else(is.na(relevant$nb_of_sizes), 5, relevant$nb_of_sizes)
      lifespan_w = as.integer(relevant$lifespan_days / DAYS_IN_WEEK)
      max_UI_week_life = as.integer((window_end - relevant$in_shop_week_date) / DAYS_IN_WEEK)

      class(in_shop_week_date) = "Date"

      ## this one is not on pm level but comes from param file which is on that level
      regional_trail_size = max(relevant$reg_trail_size, na.rm = TRUE)
      min_reg_trail_qty = regional_trail_size * sum(fc_lifespan_demand)

      max_EOP = relevant$max_EOP
      max_number_of_sizes = relevant$max_number_of_sizes

      trail_size_pct = relevant$min_trail_size_pct
      ground_size_pct = relevant$min_ground_size_pct
      min_split_timing = relevant$min_split_timing
      max_number_of_trails = relevant$max_number_of_trails
      qos_target = relevant$qos_target
      M = relevant$M
      weeks_of_coverage = relevant$weeks_of_coverage
      max_s_percent = relevant$max_s_percent

      last_trail_timing = pmin(relevant$last_trail_timing, lifespan_w - 1)
      safety_stock = list()
      weekly_demand = list()
      service_level = list()
      weekly_error = list()
      def_orders_quantity = list()
      def_orders_week = list()
      sim_orders_quantity = list()
      sim_orders_week = list()
      def_order_no_sim_week = list()
      safety_stock_buffer = list()
      simulated_stock = list()
      stock_margin = list()
      week_to_start_adjusted = week_to_start + lead_time_weeks
      lifespan_w_adjusted = lifespan_w - lead_time_weeks
      index_adjusted = list()
      should_deactivate_minimum_trail <- c()
      should_deactivate_ooq <- c()

      avg_demand <- numeric(n_countries)

      week_level_params_current = list()

      for (p in 1:n_countries) {
        this_wdf = wdf_output_tl[wdf_output_tl$article_id == article_id[p] &
                                  wdf_output_tl$pm == pm[p] &
                                  wdf_output_tl$week_life <= lifespan_w[p],]

        weekly_demand[[p]] = pmax(this_wdf$week_demand, 0.01)
        weekly_error[[p]] = this_wdf$historical_cumulative_error

        safety_stock_res = calculate_safety_stock(weekly_demand[[p]],
                                                  ri_flag[[p]],
                                                  nb_of_sizes[p],
                                                  max_number_of_sizes[p],
                                                  nb_of_stores_predicted[p],
                                                  max_EOP[p],
                                                  weeks_of_coverage[p],
                                                  iaq[p])
        avg_demand[p] = safety_stock_res$avg_demand

        safety_stock[[p]] = safety_stock_res$safety_stock

        ## extracting the 95% upper bound of demand outcome for each week, which will be used to compute our 95% safety stock
        service_level[[p]] = calculate_service_level(safety_stock[[p]],
                                                    weekly_demand[[p]],
                                                    weekly_error[[p]],
                                                    qos_target[p])

        week_level_params_current[[p]] = data.frame(article_id = article_id[p],
                                                    pm = pm[p],
                                                    week_life = 1:lifespan_w[p],
                                                    weekly_demand = weekly_demand[[p]],
                                                    safety_stock = safety_stock[[p]],
                                                    safety_stock_buffer = safety_stock[[p]]
                                                    + service_level[[p]]
                                                    + weekly_demand[[p]],
                                                    weekly_error = weekly_error[[p]])

        # Get the safety stock buffer constraint value for each pm
        safety_stock_buffer_pm = rep(0, total_lifespan_w)
        safety_stock_buffer_pm[week_to_start[p] + 0:(lifespan_w[p] - 1)] = (
                                      safety_stock[[p]]
                                      + service_level[[p]]
                                      + weekly_demand[[p]])

        # Get the speed constraint value for each pm
        speed_constraint_pm <- rep(0, total_lifespan_w)
        # The speed constraint can be rewritten as Stock >= Demand / Max Speed
        speed_constraint_pm[week_to_start[p] + 0:(lifespan_w[p] - 1)] <- weekly_demand[[p]] / max_s_percent[p]

        # If we aggregate the safety stock and the speed constraint together, we get that
        # Stock >= max(Safety Stock, Demand / Max Speed)
        safety_stock_buffer[[p]] <- pmax(safety_stock_buffer_pm,speed_constraint_pm)

        # Get def order as a week vector and quantity vector
        formatted_def_order = format_order_table(def_orders,
                                p,
                                article_id,
                                pm,
                                start_date,
                                in_shop_week_date[p],
                                is_def_order_active)

        def_orders_quantity[[p]] = formatted_def_order$orders_quantity
        def_orders_week[[p]] = formatted_def_order$orders_week

        # Get sim order as a week vector and quantity vector
        formatted_sim_order = format_order_table(sim_orders,
                                p,
                                article_id,
                                pm,
                                start_date,
                                in_shop_week_date[p],
                                is_def_order_active)

        sim_orders_week[[p]] = formatted_sim_order$orders_week

        def_order_no_sim_week[[p]] = setdiff(def_orders_week[[p]], sim_orders_week[[p]])

        # Get an index containing the week starting from `week_to_start_adjusted`
        # and `week_to_start + lifespan_w - 1`
        index_adjusted[[p]] = week_to_start_adjusted[p] - 1 + 1:lifespan_w_adjusted[p]

        # Get simulated stock
        simulated_stock[[p]] = get_simulated_stock(total_lifespan_w,
                                    lead_time_weeks[p],
                                    def_orders_quantity[[p]],
                                    def_orders_week[[p]],
                                    ingoing_stock[p],
                                    week_to_start[p],
                                    week_to_start_adjusted[p],
                                    lifespan_w_adjusted[p],
                                    weekly_demand[[p]])

      # Get the stock margin for each week
      stock_margin[[p]] <- simulated_stock[[p]] - safety_stock_buffer[[p]]
      }

      week_level_params <- rbind(week_level_params,
                                do.call(rbind, week_level_params_current))

      init_safety_stock <- lapply(safety_stock, first) %>% unlist()

      min_gt <- get_min_gt(init_safety_stock,
                          fc_lifespan_demand,
                          core_extended,
                          nb_of_sizes,
                          tmq_percent,
                          trail_size_pct,
                          ground_size_pct)

      min_ground_qty = min_gt$min_ground_qty
      min_trail_qty = min_gt$min_trail_qty

      maximum_ground_trail_quantity <- get_maximum_ground_trail_quantity(
        n_countries,
        min_ground_qty,
        min_trail_qty,
        min_reg_trail_qty,
        def_orders_quantity,
        safety_stock_buffer,
        fc_lifespan_demand,
        QUANTIFICATION_LIFESPAN_TOLERANCE,
        job_id
        )

      ## Setting up variables ####################################################
      # For each country there are 3 blocks of variables
      #   Cp = [O1:Ow|Q1:Qw|S1:Sw]
      # where w = total_lifespan_w
      # then this pattern is repeated fo each country
      #  Var = [C1|C2|..|Cp]
      # for p = n_countries
      var_types <- rep(c("binary", "real", "real"), each = total_lifespan_w) %>%
        rep(times = n_countries)
      vars_per_country <- 3 * total_lifespan_w

      ## Calculate lead time related variables ############################

      if (is_def_order_active) {
        for (p in 1:n_countries) {

          # Get the lower bound for activation of pm
          min_trail_lower_bound <- -min_trail_qty[p]
          order_lower_bound <- -init_safety_stock[p] * MINIMUM_TRAIL_ACTIVATION_MULTIPLIER

          # Determine if we need to deactivate min trail for pm p
          should_deactivate_minimum_trail[p] <- deactivate_min_trail(stock_margin[[p]],
                                  index_adjusted[[p]],
                                  min_trail_lower_bound,
                                  order_lower_bound,
                                  pm[p]
                                  )
          # Determine if we need to deactivate ooq for pm p
          should_deactivate_ooq[p] <- deactivate_ooq(stock_margin[[p]],
                                            lifespan_w_adjusted[p],
                                            week_to_start[p],
                                            index_adjusted[[p]],
                                            week_to_start_adjusted[p],
                                            def_order_no_sim_week[[p]],
                                            order_lower_bound,
                                            pm[p]
                                            )
        }

        # Calculate emergency order
        emergency_orders_week <- get_emergency_orders_week(total_lifespan_w,
                                                  n_countries,
                                                  pm,
                                                  lifespan_w,
                                                  week_to_start,
                                                  lead_time_weeks,
                                                  week_to_start_adjusted,
                                                  index_adjusted,
                                                  weekly_demand,
                                                  stock_margin,
                                                  def_orders_week,
                                                  sim_orders_week,
                                                  min_split_timing[1],
                                                  should_deactivate_ooq
                                                  )
      } else {
        should_deactivate_minimum_trail <- rep(FALSE, n_countries)
        should_deactivate_ooq <- rep(FALSE, n_countries)
        emergency_orders_week <- vector(mode = "list", length = n_countries)
      }

      ## Block of constraints for each individual pm ############################
      mat_list <- list()
      constr_value_vec <- c()
      constr_sign_vec <- c()
      obj_vec <- numeric(vars_per_country * n_countries)

      for (p in 1:n_countries) {

        if (should_deactivate_ooq[p] & is_def_order_active) {
          # Remove constraint if ooq is deactivate for pm p
          these_constr <- disabled_ooq_constraints(total_lifespan_w,
                                                simulated_stock[[p]],
                                                def_orders_week[[p]],
                                                def_orders_quantity[[p]])
        } else {
          these_constr <- get_one_country_constraints(total_lifespan_w,
                                                      lifespan_w[p],
                                                      week_to_start[p],
                                                      lead_time_weeks[p],
                                                      week_to_start_adjusted[p],
                                                      lifespan_w_adjusted[p],
                                                      index_adjusted[[p]],
                                                      fc_lifespan_demand[p],
                                                      weekly_demand[[p]],
                                                      safety_stock[[p]],
                                                      service_level[[p]],
                                                      ingoing_stock[p],
                                                      simulated_stock[[p]],
                                                      safety_stock_buffer[[p]],
                                                      last_trail_timing[p],
                                                      max_UI_week_life[p],
                                                      max_number_of_trails[p],
                                                      min_ground_qty[p],
                                                      min_trail_qty[p],
                                                      maximum_ground_trail_quantity[p],
                                                      def_orders_week[[p]],
                                                      def_orders_quantity[[p]],
                                                      sim_orders_week[[p]],
                                                      def_order_no_sim_week[[p]],
                                                      emergency_orders_week[[p]],
                                                      should_deactivate_minimum_trail[p])
        }

        mat_list[[p]] <- these_constr$constr_coeff_mat
        constr_value_vec <- c(constr_value_vec, these_constr$constr_value_vec)
        constr_sign_vec <- c(constr_sign_vec, these_constr$constr_sign_vec)
        obj_vec[(p - 1) * vars_per_country + 1:(vars_per_country)] <- these_constr$obj_vec
      }

      mat <- bdiag(mat_list)

      ## Constraint of linking and timing of orders ##############################
      ## defined as for all countries p,  Oi-2,p + 0i-1,p + sum(Oi,p') <= 1
      mat_list <- list()
      min_split = min(min_split_timing[1], total_lifespan_w)
      rows_per_country <- total_lifespan_w

      base_constr <- matrix(0, total_lifespan_w, n_countries * total_lifespan_w * 3)

      ## Setting the link part of the constraint
      for (p in 1:n_countries) {

        diag_value <- rep(1 / (n_countries + 1), total_lifespan_w)
        diag_value[1:total_lifespan_w <= week_to_start_adjusted[p]] <- 0
        oi_vars <- diag(diag_value)

        base_constr[, 1:total_lifespan_w + (p - 1) * total_lifespan_w * 3] <- oi_vars
      }

      ## Setting "blockers" for min split timing
      for (p in 1:n_countries) {
        oi_vars <- matrix(0, total_lifespan_w, total_lifespan_w)

      # The element to set to one are the `min_split - 1` diagonals below the principal diagonal.
      # The starting rows are the one starting from `week_to_start_adjusted`
        element_to_set_to_one <- row(oi_vars) - col(oi_vars) > 0 &
                          row(oi_vars) - col(oi_vars) < min_split &
                          row(oi_vars) > week_to_start_adjusted[p]
        oi_vars[element_to_set_to_one] <- 1

        this_constr <- base_constr
        ind <- 1:total_lifespan_w + (p - 1) * total_lifespan_w * 3
        this_constr[, ind] <- this_constr[, ind] + oi_vars

        mat_list[[p]] <- this_constr
      }

      mat_link <- do.call(rbind, mat_list)

      # Remove link constraint because it can led to impossible ooq
      # (e.g. having to place to def order with a splitting lower than min_split_timing)
      # TODO: Remove is_def_order_active in the future
      if (is_def_order_active) {

        # Remove link constraint between emergency order and def order
        mat_link <- remove_link_constraint(emergency_orders_week,
                                              def_orders_week,
                                              mat_link,
                                              n_countries,
                                              rows_per_country,
                                              vars_per_country,
                                              min_split)

        # Remove link constraint between def order and def order
        mat_link <- remove_link_constraint(def_orders_week,
                                              def_orders_week,
                                              mat_link,
                                              n_countries,
                                              rows_per_country,
                                              vars_per_country,
                                              min_split)

        # Remove link constraint between emergency order and emergency order
        mat_link <- remove_link_constraint(emergency_orders_week,
                                              emergency_orders_week,
                                              mat_link,
                                              n_countries,
                                              rows_per_country,
                                              vars_per_country,
                                              min_split)
      }

      constr_value_vec <- c(constr_value_vec, rep(1, n_countries * total_lifespan_w))
      constr_sign_vec <- c(constr_sign_vec, rep("<=", n_countries * total_lifespan_w))


      ## REGIONAL TRAIL CONSTRAINT ##############################################
      # -- At all trail times, we want sum(Qi) >= Op * min_regional_trail
      # for all p in countries
      mat_list <- list()
      base_constr <- kronecker(matrix(1, 1, n_countries),
                              cbind(matrix(0, total_lifespan_w, total_lifespan_w),
                                    diag(1, total_lifespan_w),
                                    matrix(0, total_lifespan_w, total_lifespan_w)))

      for (p in 1:n_countries) {
        if (!should_deactivate_minimum_trail[p]) {

          oi_vars <- diag(-min_reg_trail_qty, total_lifespan_w)

          if (length(def_orders_week[[p]]) > 0) {
            oi_vars[col(oi_vars) == row(oi_vars)][def_orders_week[[p]]] <- 0
          }

          oi_vars[row(oi_vars) <= week_to_start_adjusted[p]] <- 0

          this_constr <- base_constr
          this_constr[, 1:total_lifespan_w + (p - 1) * total_lifespan_w * 3] <- oi_vars

          mat_list[[p]] <- this_constr
        }
      }

      mat_region <- do.call(rbind, mat_list)

      if (!is.null(mat_region)) {
        constr_value_vec <- c(constr_value_vec, rep(0, nrow(mat_region)))
        constr_sign_vec <- c(constr_sign_vec, rep(">=", nrow(mat_region)))
      }

      ## Putting all constraints together ########################################
      constr_coeff_mat <- rbind(mat,
                                mat_link,
                                mat_region)
      nb_of_constraints <- nrow(constr_coeff_mat)

      flog.info(
        paste(
          'OOQ solver number of countries:',
          n_countries,
          ', number of constraints:',
          nb_of_constraints,
          ', size of the lifespan:',
          total_lifespan_w
        ),
        scope="OOQ",
        job_id = job_id
      )

      # Used to log time to solve the problem. The speed reports from the solvers
      # only show time spend on solving the problem and not the overhead.
      start <- Sys.time()

      # List of optional params for Gurobi. Put OutputFlag = 0 if we do not want logging.
      params <- list(TimeLimit = GUROBI_TIMEOUT_VALUE)

      ## Solution and result extraction ##########################################
      if (should_solve_with_cloud_based_solver(n_countries, total_lifespan_w)) {
        solution_vector <- solve_with_cloud_based_solver(n_countries,
                                                         total_lifespan_w,
                                                         job_id,
                                                         constr_coeff_mat,
                                                         obj_vec,
                                                         constr_value_vec,
                                                         constr_sign_vec,
                                                         params)

      } else {
        flog.info("Solving with lpSolveAPI", scope = "OOQ Helpers", job_id = job_id)
        lpsolve_output <- solve_ooq(var_types = var_types,
                                     obj_vec = obj_vec,
                                     constr_coeff_mat = constr_coeff_mat,
                                     constr_sign_vec = constr_sign_vec,
                                     constr_value_vec = constr_value_vec,
                                     job_id = job_id)

        # The lpSolveAPI provides both the constraint and variable values. We only
        # need the variable values, so remove the constraint values.
        solution_vector <- lpsolve_output$solution_vector[(nb_of_constraints + 1):length(lpsolve_output$solution_vector)]

        # If the lpSolve did not find an optimal solution, we resort to calling
        # Gurobi as a last resort to avoid a suboptimal solution. Because lpSolve
        # has already timed out once, this can lead to slow results
        if (lpsolve_output$rerun_problem_with_cloud_based_solver){
          solution_vector <- solve_with_cloud_based_solver(n_countries,
                                                           total_lifespan_w,
                                                           job_id,
                                                           constr_coeff_mat,
                                                           obj_vec,
                                                           constr_value_vec,
                                                           constr_sign_vec,
                                                           params)
        }
      }

      # Logging the time spend solving the problem.
      flog.info(paste(
        'Time to solve with',
        n_countries,
        'countries is',
        as.numeric(Sys.time() - start, units='secs'), 'seconds')
        , scope = "OOQ Helpers", job_id = job_id)

      # `solution_vector` != null means that we found an optimal solution
      if (!is.null(solution_vector)) {
        # Splitting optimal variables by country.
        variables_matrix <- matrix(solution_vector, n_countries, vars_per_country, byrow = TRUE)
        # Splitting Orders, Quantity, and Stock in to 3 matrix.
        ## Order matrix correspond to the `1:total_lifespan_w` columns and represent the weeks we should make order.
        order_matrix <- variables_matrix[, 1:total_lifespan_w, drop = FALSE]
        ## Quantity matrix correspond to the `total_lifespan_w:2*total_lifespan_w` columns and represent the quantity per weeks we should order.
        quantity_matrix <- variables_matrix[, 1:total_lifespan_w + total_lifespan_w, drop = FALSE]
        ## Stock matrix correspond to the `2*total_lifespan_w:3*total_lifespan_w` columns and represent the stock per weeks we should order.
        stock_matrix <- variables_matrix[, 1:total_lifespan_w + 2*total_lifespan_w, drop = FALSE]
        # Remove floating point artefacts from the solver.
        order_matrix[order_matrix <= ENVIRONMENT_VARIABLES$INTEGER_FEASIBILITY_TOLERANCE] <- 0
        quantity_matrix <- order_matrix * quantity_matrix

        # Calculating average speed for the given solution
        average_speed <- get_average_speed(
          n_countries,
          weekly_demand,
          lead_time_weeks,
          lifespan_w_adjusted,
          stock_matrix,
          index_adjusted
        )

        results_current_article <- reshape_ooq_result(order_matrix,
                                                  quantity_matrix,
                                                  article_id,
                                                  pm,
                                                  week_to_start,
                                                  in_shop_week_date,
                                                  average_speed)

      } else {
        # OOQ did not find any optimal solution. In this case, we can add
        # a buffer of 30% and simply a ground for every pm as a solution.
        flog.warn(
          "OOQ did not find any solution. Only solution is going
          to be suggesting placing ground orders or at lead time of quantity
          130% * remaining demand for every pm",
          scope="OOQ",
          job_id = job_id
        )

        remaining_lifespan_demand <- c()
        for (p in 1:n_countries) {
          remaining_lifespan_demand[[p]] <- rev(cumsum(weekly_demand[[p]]))[week_to_start_adjusted[[p]]]
        }

        results_current_article = data.frame(
          article_id = article_id,
          pm = pm,
          quantity = remaining_lifespan_demand * FALLBACK_LIFESPAN_DEMAND_MULTIPLIER,
          average_speed = 1,
          ooq = remaining_lifespan_demand * FALLBACK_LIFESPAN_DEMAND_MULTIPLIER,
          order_number = 1,
          week_life = 1 + lead_time_weeks,
          order_in_shop_week_date = in_shop_week_date + lead_time_weeks * DAYS_IN_WEEK
        )
      }

      result = rbind(result, results_current_article)

      # add some statistics on pm level
      pm_level_params_current <- tibble(article_id = article_id,
                                        pm = pm,
                                        min_reg_trail_qty = min_reg_trail_qty,
                                        min_ground_qty = min_ground_qty,
                                        min_trail_qty = min_trail_qty,
                                        tmq_percent = tmq_percent,
                                        nb_of_sizes = nb_of_sizes,
                                        all_store_quantity = nb_of_sizes / tmq_percent,
                                        nb_of_stores_predicted = nb_of_stores_predicted,
                                        iaq = iaq,
                                        iaq_one_store = iaq_one_store,
                                        core_extended = core_extended,
                                        avg_demand)
      pm_level_params = rbind(pm_level_params, pm_level_params_current)
    }
  } else {
    flog.warn(
      "OOQ has found no article to quantify",
      scope = "OOQ",
      job_id = job_id
    )

    result <- data.frame(article_id = character(),
                        pm = numeric(),
                        quantity = numeric(),
                        average_speed = numeric(),
                        ooq = numeric(),
                        order_number = numeric(),
                        week_life = numeric(),
                        order_in_shop_week_date = as.Date(character()))
  }

  output = list(
    orders = list(
      data = result %>% as_tibble %>% mutate_if(is.factor, as.character),
      pm_level_params = pm_level_params,
      week_level_params = week_level_params,
      info = NULL
    )
  )

  return(output)
}
