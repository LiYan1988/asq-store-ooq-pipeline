
ROUNDING_PRECISION <- 8

#' @title Execute OOQ and update the asq_job.
#'
#' @param asq_job Object that represents the current quantification,
#'                contains all relevant data and will store the results.
#'
#' @return None, though the state of the asq_job is changed.
#'
process_job_py_ooq = function(asq_job) {
  # Log start and timing.
  flog.info("Execute PyOOQ.", scope = "OOQ", job_id=asq_job$job_id)
  TIMING$start("ooq", asq_job$job_id)

  # TODO: Update seasonless stock with one more week of data.
  #       Rewrite SSIM module to ensure it simulates ingoing stock until ISW.

  # Extract sales and stock information relevant for OOQ (only target).
  weekly_seasonless_demand =
    asq_job$get_seasonless_data() %>%
    filter(main_pas_code == asq_job$target_pas_code) %>%
    mutate(
      forecasted_demand = round(forecasted_demand, ROUNDING_PRECISION),
      historical_cumulative_error_on_demand = round(historical_cumulative_error_on_demand, ROUNDING_PRECISION)
    ) %>%
    select(
      planning_market_id,
      week_life,
      week_name,
      forecasted_demand,
      historical_cumulative_error_on_demand
    )

  # The below set of code is from Niel's Original work. This aim to check if any
  # PM with an *ancestor* ends up having NA as an ingoing stock value.
  # If so, the call is failed here. If a PM with no-ancestors has NA ingoing stock,
  # the NA is chaged to zero and the call is continued
  # Adding pm features into ingoing stock.
  # The OOQ module only provides orders for target season
  target_season = max(asq_job$planning_market_features$season_id)
  target_pas_code = (asq_job$article_features %>% filter(season_id == target_season))$pas_code
  ingoing_stock_arg <- asq_job$get_seasonless_data() %>%
    left_join(
      asq_job$planning_market_features %>%
        filter(season_id == target_season) %>%
        select(planning_market_id, in_shop_week, lead_time_week),
      by = c("planning_market_id")
    )

  # Get the last week historic stock from `ingoing_stock_arg`.
  # Find which pm has no ancestor until max between lead time or in shop week date.
  has_no_ancestors_with_stock <- ingoing_stock_arg %>%
    filter(week_name < pmax(in_shop_week, lead_time_week)) %>%
    group_by(planning_market_id) %>%
    summarize(have_no_stock_history = all(is.na(forecasted_outgoing_stock)))
  ingoing_stock_arg <- ingoing_stock_arg %>%
    filter(week_name == pmax(in_shop_week, lead_time_week))



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

  # Derive ingoing stock for PyOOQ.
  ingoing_stock <-
    asq_job$planning_market_features %>%
    filter(pas_code == asq_job$target_pas_code) %>%
    group_by(planning_market_id) %>%
    mutate(ingoing_stock_week = max(coalesce(in_shop_week, in_shop_week_ri), lead_time_week)) %>%
    ungroup() %>%
    left_join(asq_job$get_seasonless_data(), by=c("planning_market_id", "ingoing_stock_week" = "week_name")) %>%
    select(planning_market_id, ingoing_stock_week, forecasted_ingoing_stock) %>%
    transform(ingoing_stock_week = as.numeric(ingoing_stock_week))

  # Number of stores is required for TMQ% (technical minimum quantity %),
  # and IAQ (initial allocation quantity).
  forecasted_store_number_for_target =
    asq_job$get_forecasted_number_of_stores_by_planning_market() %>%
    filter(pas_code == asq_job$target_pas_code)

  # Extract planning market data, relevant for OOQ.
  planning_market_features_for_target =
    asq_job$planning_market_features %>%
    filter(pas_code == asq_job$target_pas_code) %>%
    mutate(
      lifespan = coalesce(lifespan, lifespan_ri)
    ) %>%
    select(
      planning_market_id,
      in_shop_week,
      lead_time_week,
      lifespan,
      core_extended_flag,
      nb_of_sizes,
      iaq_one_store,
      is_ri_user
    )

    # Send all orders.
    orders_for_target =
      asq_job$get_orders() %>%
      filter(pas_code == asq_job$target_pas_code) %>%
      select(
        planning_market_id,
        week_name,
        order_type,
        order_size
      )

    # Send paramenters, relevant to OOQ's execution.
    ooq_parameters = asq_job$resources$ooq_params

    # Call the Py OOQ module to solve the problem.
    raw_ooq_result = run_python_ooq(
      weekly_seasonless_demand=weekly_seasonless_demand,
      orders_for_target=orders_for_target,
      planning_market_features=planning_market_features_for_target,
      forecast_store_numbers=forecasted_store_number_for_target,
      ooq_parameters=ooq_parameters,
      ingoing_stock=ingoing_stock,
      should_plot_solution=FALSE
    )

#####################################################################
    base_dump_path = "/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-2/assortment/r_agent/output/python_r_benchmark/csv/f384e762-eeb2-eb11-94b3-501ac5e6ac5c/"
    # base_dump_path = "/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-1/assortment/r_agent/output/python_r_benchmark/csv/0c160a7d-dfb7-eb11-94b3-501ac5e6ac5c/"
    write.csv(weekly_seasonless_demand, file = paste0(base_dump_path, "weekly_seasonless_demand.csv"), quote = TRUE, row.names=FALSE)
    write.csv(orders_for_target, file = paste0(base_dump_path, "orders_for_target.csv"), quote = TRUE, row.names=FALSE)
    write.csv(planning_market_features_for_target, file = paste0(base_dump_path, "planning_market_features_for_target.csv"), quote = TRUE, row.names=FALSE)
    write.csv(forecasted_store_number_for_target, file = paste0(base_dump_path, "forecasted_store_number_for_target.csv"), quote = TRUE, row.names=FALSE)
    write.csv(ooq_parameters, file = paste0(base_dump_path, "ooq_parameters.csv"), quote = TRUE, row.names=FALSE)
    write.csv(ingoing_stock, file = paste0(base_dump_path, "ingoing_stock.csv"), quote = TRUE, row.names=FALSE)
#####################################################################

    # Transform OOQ's result to the wanted format. For the time being,
    # we only want the orders dataframe.
    ooq_result = c()
    # raw_ooq_result is a list of list with first part being OOQ
    # suggestion and second the list of PM names for messages
    # (if not emppty)
    ooq_result$orders$data = raw_ooq_result[[1]]
    pms_with_no_ooq_required = raw_ooq_result[[2]]
    pm_level_params_trace = raw_ooq_result[[3]]
    week_level_params_trace = raw_ooq_result[[4]]

    # Dump OOQ inputs to Redis, to expose as traces in Cockpit.
    TRACE$dump(
      step = "orders_before_OOQ",
      table = asq_job$get_orders() %>%
        left_join(asq_job$planning_market_map, by = c("planning_market_id")),
      job_id = asq_job$job_id
    )
    if (length(pms_with_no_ooq_required) > 0)
    {
      add_enough_stock_user_message(
        pms_with_no_ooq_required,
        asq_job
      )
    }

    # Check if there is suggested orders
    number_of_rows = nrow(ooq_result$orders$data)
    has_rows = !is.null(number_of_rows) && number_of_rows
    if (has_rows) {
      # unlist the planning_market_id named list as it is the list of list due to issue in converting pandas Df to R named list
      ooq_result$orders$data$planning_market_id = unlist(ooq_result$orders$data$planning_market_id)
      # convert the named list to tibble so that dplyr dataframe operations can be done without any risk
      ooq_result$orders$data = as_tibble(ooq_result$orders$data)
      ooq_result$orders$data = remove_orders_with_less_than_minimum_order_quantity(
        ooq_result$orders$data, OOQ_MINIMUM_ORDER_QUANTITY)

      # TODO Remove Def orders from Python side

      # Log OOQ module completion.
      flog.info(
        "OOQ module executed in Python, updating asq_job.",
        scope="OOQ",
        job_id = asq_job$job_id
      )

      # Append the OOQ suggestion with a type designation "ooq"
      asq_job$append_orders(
        planning_market_ids = unlist(ooq_result$orders$data$planning_market_id),
        pas_codes = ooq_result$orders$data$pas_code,
        week_names = ooq_result$orders$data$order_in_shop_week_date,
        order_types = "ooq", # These orders are necessarily of type "OOQ"
        order_numbers = ooq_result$orders$data$order_number,
        order_sizes = ooq_result$orders$data$order_quantity,
        order_optimal_quantities = ooq_result$orders$data$total_ooq_quantity_for_planning_market,
        week_life = ooq_result$orders$data$week_name
      )

      # Finalize execution by dumping traces, and stopping the timer.
      TRACE$dump(
        step = "OOQ_planning_market_level_parameters",
        table = list(pm_level_params_trace %>%
          left_join(asq_job$planning_market_map, by = c("pm" = "planning_market_id")) %>%
           rename(num_stores_adjusted_by_ooq = nb_of_stores_predicted)),
        job_id = asq_job$job_id
      )
      TRACE$dump(
        step = "OOQ_week_level_parameters",
        table = list(week_level_params_trace %>%
          left_join(asq_job$planning_market_map, by = c("pm" = "planning_market_id"))),
        job_id = asq_job$job_id
      )
      TRACE$dump(
        step = "orders_after_OOQ",
        table = asq_job$get_orders() %>%
          left_join(asq_job$planning_market_map, by = c("planning_market_id")),
        job_id = asq_job$job_id
      )
      # TODO: Update the unified log based upon the outputs.
      TIMING$end("ooq", asq_job$job_id)

    } else {
      flog.info(
        "OOQ has suggested zero orders!",
        scope="OOQ",
        job_id = asq_job$job_id
      )
    }
}


#' Add the enough ingoing stock / OOQ may not be needed user message per season and planning market
#'
#' @param planning_market_enough_stocks 'planning_market_id' having enough stocks
#' @param asq_job The ASQ job object.
#' @param message_type_name Message type i.e., BuyingSuggestedButNotNeeded'.
#'
#' @return None, but the asq_job object gets modified with the added enough ingoing stock user message.
add_enough_stock_user_message_type_per_season_pm <- function(
  planning_market_enough_stocks,
  asq_job,
  message_type_name
){
  asq_job$append_user_message_for_ui(
    message_type_name = message_type_name,
    pascode = asq_job$target_pas_code,
    pm_id = planning_market_enough_stocks,
  )
}

#' Add the enough ingoing stock / buying may not be needed user message for all the affected planning markets.
#'
#' @param planning_markets_with_no_ooq_required List with 'planning_market_id's having enough stocks.
#' @param asq_job The ASQ job object.
#' @param message_source Message source i.e., PyOOQ'.
#'
#' @return None, but the asq_job object gets modified with the added enough ingoing stock user message.
#' (one per planning market id tuple).
add_enough_stock_user_message <- function(
  planning_markets_with_no_ooq_required,
  asq_job
) {
  lapply(planning_markets_with_no_ooq_required,
         function(pm_id) add_enough_stock_user_message_type_per_season_pm(pm_id, asq_job, "BuyingSuggestedButNotNeeded"))
}


#' Remove rows with buying suggestion having less than mininum_order_quantity
#' This function also make sure to fail if a planning market id in buying_suggestion_df gets completely filtered out.
#' @param buying_suggestion_df Dataframe containing buying suggestions against planning markets at week level.
#' @param mininum_order_quantity minimum allowed order quantity comes from configuration
#'
#' @return buying_suggestion_df with filtered out rows having less than mininum_order_quantity value. Very rare case though.
remove_orders_with_less_than_minimum_order_quantity <- function(buying_suggestion_df, mininum_order_quantity)
{
  pms_with_buying_suggestion = buying_suggestion_df %>%
                                distinct(planning_market_id)
  buying_suggestion_df = buying_suggestion_df %>%
                          filter(order_quantity >= mininum_order_quantity)
  pms_remaining_with_buying_suggestion = buying_suggestion_df %>%
                                          distinct(planning_market_id)

  if(nrow(pms_with_buying_suggestion) != nrow(pms_remaining_with_buying_suggestion))
  {
    # setdiff is dplyr method which returns items from first params (df) which are not present in second params(df)
    planning_markets_with_less_than_one_order = setdiff(pms_with_buying_suggestion, pms_remaining_with_buying_suggestion)
    stop(
      sprintf("OOQ has suggested orders of size less than the minimum acceptable value (OOQ_MINIMUM_ORDER_QUANTITY=%s)
              for all the weeks at a Planning market(s): %s.",
              mininum_order_quantity, planning_markets_with_less_than_one_order)
    )
  }

  return(buying_suggestion_df)
}
