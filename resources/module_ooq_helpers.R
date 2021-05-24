# Helper functions for the OOQ module.

# Number of days in a week
DAYS_IN_WEEK = 7
# Number of weeks until the end of lifespan where no buying suggestions are allowed
LIFESPAN_ENDING_INTERVAL = 4
# Percentage of the initial safety stock for relaxing the minimum trail constraint
MINIMUM_INITIAL_SAFETY_ACTIVATION_OOQ = 0.1
# Percentage of the average regional weekly demand for relaxing the minimum trail constraint
MINIMUM_REGIONAL_ACTIVATION_OOQ = 0.01
# Total solver time for 4 regions should not exceed 30 seconds. However, we typically see
# one difficult region and a couple of easier ones for each call. As such, we allow up to
# 10 seconds, as 4*10 seconds would never happen, but having a single solver taking between
# 7 and 10 seconds could happen.
SOLVER_TIMEOUT = 10

#' Calculate Safety Stock
#'
#' Given weekly demand in lifespan, type of article (normal/ri), number
#' of sizes and number of stores predicted, calculate safety stock during
#' lifetime.
#'
#' @param weekly_demand a numeric vector
#' @param is_ri a binary value whether article is RI: 1 for RI, 0 for non-RI
#' @param nb_of_sizes a numeric value representing number of sizes the article
#'        has
#' @param max_number_of_sizes integer for maximal possible value for number of sizes used in computations
#' @param nb_of_stores_predicted a numeric value representing the number
#'        of stores the article is allocated to
#' @param max_EOP factor for maximal possible value for End of Piece
#' @param weeks_of_coverage for number of weeks we want to cover on top of IAQ
#' @param iaq initial allocation quantity
#' @return A numeric vector of safety stocks.
#'
calculate_safety_stock <- function(
  weekly_demand,
  is_ri,
  nb_of_sizes,
  max_number_of_sizes,
  nb_of_stores_predicted,
  max_EOP,
  weeks_of_coverage,
  iaq
  ) {
  # calculate
  lifespan_in_weeks <- length(weekly_demand)
  fc_lifespan_demand <- sum(weekly_demand)

  ## We create a factor which is based on the expected remaining share of demand to sell that will be used to decrease safety stock later on.
  ## Except for RIs where it remains constant
  decreasing_factor = (fc_lifespan_demand - cumsum(weekly_demand)) / fc_lifespan_demand * (1 - is_ri) + is_ri
  # The decreasing factor is not applied for 1st week
  decreasing_factor[1] = 1

  ## end of piece : client definition
  eop = min(fc_lifespan_demand * max_EOP, min(nb_of_sizes, max_number_of_sizes) * nb_of_stores_predicted)

  ## PM-level IAQ to imitate the behavior on store level -- as Allo does

  ## This is the number of weeks that we will use as coverage at the beginning
  # of lifespan and decreasing onwards (decreasing factor) -- this is a disguised
  # confidence buffer, especially for Running items.
  coverage_factor = min(0.25 * lifespan_in_weeks, weeks_of_coverage) - 1

  safety_stock = (coverage_factor * fc_lifespan_demand / lifespan_in_weeks - eop + iaq) * decreasing_factor + pmin(eop, iaq)

  return(list(safety_stock = safety_stock,
              eop = eop,
              avg_demand = fc_lifespan_demand / lifespan_in_weeks))
}

#' Calculated Upper Bound and Service Level for Weekly Demand
#'
#' @param safety_stock a numeric vector
#' @param weekly_demand a numeric vector
#' @param weekly_error a numeric vector
#' @param qos_target a numeric value in [0, 1]
#'
#' @return A numeric vector of service level
#'
calculate_service_level <- function(safety_stock, weekly_demand, weekly_error, qos_target) {
  # Compute 95th percentile of a gamma distribution of mean remaining sales (incl. current week)
  # and standard deviation max weekly error * remaining sales (incl. current week)
  # which will be used to compute 95% safety stock
  mu <- rev(cumsum(rev(weekly_demand)))
  max_weekly_error <- max(weekly_error)
  sigma <- mu * max_weekly_error

  qos <- qgamma(qos_target,
                shape = mu ^ 2 / (sigma ^ 2),
                scale = (sigma ^ 2) / mu)

  mean_of_gamma_distrib <- mu

  fc_lifespan_demand <- sum(weekly_demand)

  # extracting the 95% upper bound of demand outcome
  # for each week, which will be used to compute our 95% safety stock
  weekly_demand_qos = pmin(qos, fc_lifespan_demand)
  service_level = weekly_demand_qos - mean_of_gamma_distrib

  return(service_level)
}


#' Calculate Minimal Ground and Trail Orders
#'
#' Function calculates minimal values of ground and trail orders for a vector
#' of PMs.
#'
#' @param init_safety_stock a numeric value or vector
#' @param fc_lifespan_demand a numeric value or vector
#' @param core_extended a numeric value or vector
#' @param nb_of_sizes a numeric value or vector
#' @param tmq_percent a numeric value or vector
#' @param trail_size_pct a numeric value or vector
#' @param gropund_size_pct a numeric value or vector
#'
#' @return A two-element list:
#'         \itemize{
#'           \item{min_ground_qty}{a numeric vector/value with minimal ground
#'             quantities}
#'           \item{min_trail_qty}{a numeric vector/value with minimal trail
#'             quantities}
#'         }
#'
get_min_gt <- function(
  init_safety_stock,
  fc_lifespan_demand,
  core_extended,
  nb_of_sizes,
  tmq_percent,
  trail_size_pct,
  ground_size_pct
  ) {

  min_ground_qty = if_else(
    tolower(core_extended) == "core",
    nb_of_sizes / tmq_percent,
    ground_size_pct * init_safety_stock
    )
  # The minimum trail quantity is linked to the safety stock
  # This way, 2 articles that have same lifespan demand and not same lifespan won't have same min trail size
  min_trail_qty = trail_size_pct * init_safety_stock

  return(list(min_ground_qty = min_ground_qty,
              min_trail_qty = min_trail_qty))
}

#' Create Optimal Orders Table
#'
#' The function takes results of the optimization and outputs them in readable
#' format. Additional information on planning markets, articles,
#' and calendar dates are attached to order timings and quantities.
#'
#' @param order_matrix a \code{number_of_countries} x \code{total_weeks} matrix
#'        containing binary indicators whether an order should be placed (as output
#'        by the optimization problem)
#' @param quantity_matrix a \code{number_of_countries} x \code{total_weeks} matrix
#'        containing optimal order quantities (as output by the optimization problem)
#' @param article_id a character vector of length  \code{number_of_countries}, containing
#'        article ids
#' @param pm a character vector of length  \code{number_of_countries} that contains
#'        planning market identifiers
#' @param week_to_start a numeric vector of length  \code{number_of_countries}
#'        representing the week when the article is available for sale in given
#'        market, relative to the first week in the region
#' @param in_shop_week_date a Date vector of length \code{number_of_countries}
#'        representing the
#' @param average_speed a numeric value representing the average
#'        speed of articles
#'
#' @return A tibble with the following columns:
#'         \itemize{
#'           \item{article_id}{article id}
#'           \item{pm}{planning market}
#'           \item{quantity}{quantity to be ordered in this batch}
#'           \item{average_speed}{}
#'           \item{ooq}{total quantity to be ordered}
#'           \item{order_number}{ordinal number of order of given product in given pm}
#'           \item{week_life}{when should the order be placed relative
#'             to in-shop-week}
#'           \item{order_in_shop_week_date}{calendar date of this order}
#'         }
#'
reshape_ooq_result <- function(
  order_matrix,
  quantity_matrix,
  article_id,
  pm,
  week_to_start,
  in_shop_week_date,
  average_speed
  ) {
  quantity_matrix <- quantity_matrix * order_matrix
  # turn var_matrix into a data.frame
  quantity_matrix[which(quantity_matrix == 0)] <- NA

  # turning the matrix into tibble format
  colnames(quantity_matrix) <- 1:ncol(quantity_matrix)
  quantity_tib <- as_tibble(quantity_matrix) %>%
    cbind(article_id = article_id,
          pm = pm,
          week_to_start = week_to_start,
          in_shop_week_date = in_shop_week_date,
          average_speed = average_speed)

  # moving to long format and dropping empty orders
  quantity_long <- gather(quantity_tib, key = "total_week_life", value = "quantity",
                     - article_id, - pm, - week_to_start, - in_shop_week_date,
                     - average_speed, na.rm = TRUE)

  # calculating additional columns
  quantity_long <- quantity_long %>%
    mutate(total_week_life = as.numeric(total_week_life),
           week_life = total_week_life - week_to_start + 1,
           order_in_shop_week_date = in_shop_week_date + DAYS_IN_WEEK * (week_life - 1)) %>%
    group_by(pm) %>%
    mutate(ooq = sum(quantity),
           order_number = order(week_life)) %>%
    ungroup()

  # selecting the final variables
  res <- select(quantity_long,
                article_id,
                pm,
                quantity,
                average_speed,
                ooq,
                order_number,
                week_life,
                order_in_shop_week_date)

  return(res)
}

#' Convenience Wrapper on Mixed-Integer Solver
#'
#' Convenience function for handling communication with solver.
#'
#' @param var_types a character vector of types of variables, values have to be
#'        "binary" or "real"
#' @param obj_vec a numeric vector providing the coefficients in the objective
#'        function
#' @param constr_coeff_mat a numeric matrix containing coefficients of the constraints
#' @param constr_sign_vec a character vector providing types of constraints,
#'        values have to be "=", "<=", ">="
#' @param constr_value_vec a numeric vector providing types of constraints,
#'        values have to be "=", "<=", ">="
#' @param job_id Job ID to be used for logging.
#'
#' @return A vector of length 1 + number of constraints + number of variables
#'         with the result of the optimization in this order: objective functions
#'         value, values of constraints, values of control variables
solve_ooq <- function(
  var_types,
  obj_vec,
  constr_coeff_mat,
  constr_sign_vec,
  constr_value_vec,
  job_id
  ) {
  # checks (mostly useful when reformulating the problem)
  if (length(constr_sign_vec) != nrow(constr_coeff_mat)) {
    stop(sprintf("Vector of constraint types is not the same length as number of constraints"))
  }

  if (length(constr_value_vec) != nrow(constr_coeff_mat)) {
    stop(sprintf("Vector of constraint values is not the same length as number of constraints"))
  }

  if (length(obj_vec) != length(var_types)) {
    stop(sprintf("Number of objective value coefficients differs from number of variables"))
  }

  if (ncol(constr_coeff_mat) != length(var_types)) {
    stop(sprintf("Number of constraint coefficients differs from number of variables"))
  }

  ## preparing the linear program -- number of constraints and decision variables
  gt_optim = make.lp(nrow = nrow(constr_coeff_mat),
                     ncol = length(var_types))

  # setting types of decision variables
  set.type(gt_optim,
           which(var_types == "binary"),
           type = "binary")
  set.type(gt_optim,
           which(var_types == "real"),
           type = "real")

  # setting up constraints
  for (i in 1:ncol(constr_coeff_mat)) {
    set.column(gt_optim,
               i,
               constr_coeff_mat[, i])
  }

  set.constr.type(gt_optim, constr_sign_vec)
  set.rhs(gt_optim, constr_value_vec)

  # setting objective function
  set.objfn(gt_optim, obj_vec)

  lp.control(
    gt_optim,
    sense='min',
    break.at.first=FALSE,
    timeout=SOLVER_TIMEOUT,
    bb.depthlimit=0
    )

  solver_status_code <- solve(gt_optim)

  flog.info(
    paste(
      "OOQ objectives:",
      get.objective(gt_optim),
      "\n solution count:",
      get.solutioncount(gt_optim),
      "\n total nodes:",
      get.total.nodes(gt_optim),
      "\n total iteration:",
      get.total.iter(gt_optim)
    ),
    scope = "OOQ Helpers",
    job_id = job_id
  )
  # Rerun with cloud-based solver as a last resort
  # if we do not get a good solution with LpSolve.
  # We default the value to true and change it to
  # false if we hit an optimal or infeasible problem
  should_rerun_problem_with_cloud_based_solver <- TRUE

  if (solver_status_code == 0) {
    # solver_status_code == 0 means optimal solution found.
    flog.info("OOQ found an optimal solution.", scope = "OOQ Helpers", job_id = job_id)
    should_rerun_problem_with_cloud_based_solver <- FALSE
    return(list(solution_vector = get.primal.solution(gt_optim, orig=TRUE),
                rerun_problem_with_cloud_based_solver = should_rerun_problem_with_cloud_based_solver))

  } else if (solver_status_code == 1) {
    # solver_status_code == 1 means the model is sub-optimal.
    flog.info("OOQ found a sub-optimal solution.", scope = "OOQ Helpers", job_id = job_id)

  } else if (solver_status_code == 2) {
    # solver_status_code == 2 means the model is infeasible.
    flog.warn("OOQ optimization is infeasible.", scope = "OOQ Helpers", job_id = job_id)
    should_rerun_problem_with_cloud_based_solver <- FALSE
    return(list(solution_vector = NULL,
                rerun_problem_with_cloud_based_solver = should_rerun_problem_with_cloud_based_solver))

  } else if (solver_status_code == 3) {
    # solver_status_code == 3 means the model is unbounded.
    flog.warn("OOQ solution is unbounded.", scope = "OOQ Helpers", job_id = job_id)

  } else if (solver_status_code == 4) {
    # solver_status_code == 4 means he model is degenerate.
    flog.warn("OOQ solution is degenerate.", scope = "OOQ Helpers", job_id = job_id)

  } else if (solver_status_code == 5) {
    # solver_status_code == 5 means numerical failure encountered.
    flog.warn("OOQ has encountered a numerical failure.", scope = "OOQ Helpers", job_id = job_id)

  } else if (solver_status_code == 6) {
    # solver_status_code == 6 means the optimization was aborted.
    flog.warn("OOQ process was aborted.", scope = "OOQ Helpers", job_id = job_id)

  } else if (solver_status_code == 7) {
    # solver_status_code == 7 means the optimization timeout.
    flog.warn(paste("OOQ has not found any solution during the", SOLVER_TIMEOUT, "second allowed and has timed out."), scope = "OOQ Helpers", job_id = job_id)

  } else if (solver_status_code == 9) {
    # solver_status_code == 9 means the model was solved by presolve.
    flog.warn("OOQ was solved using the presolver.", scope = "OOQ Helpers", job_id = job_id)
    should_rerun_problem_with_cloud_based_solver <- FALSE
    return(list(solution_vector = get.primal.solution(gt_optim, orig=TRUE),
                rerun_problem_with_cloud_based_solver = should_rerun_problem_with_cloud_based_solver))

  } else if (solver_status_code == 10) {
    # solver_status_code == 10 means the branch and bound routine failed.
    flog.warn("OOQ branch and bound routine failed.", scope = "OOQ Helpers", job_id = job_id)

  } else if (solver_status_code == 11) {
    # solver_status_code == 11 means the branch and bound
    # was stopped because of a break-at-first or break-at-value.
    flog.warn("OOQ branch and bound was stopped
    because of a break-at-first or break-at-value.", scope = "OOQ Helpers", job_id = job_id)

  } else if (solver_status_code == 12) {
    # solver_status_code == 12 means feasible branch and bound solution was found
    flog.warn("OOQ found a feasible branch and bound solution.", scope = "OOQ Helpers", job_id = job_id)
    should_rerun_problem_with_cloud_based_solver <- FALSE
    return(list(solution_vector = get.primal.solution(gt_optim, orig=TRUE),
                rerun_problem_with_cloud_based_solver = should_rerun_problem_with_cloud_based_solver))

  } else if (solver_status_code == 13) {
    # solver_status_code == 13 means no feasible branch and bound solution was found
    flog.warn("OOQ branch and bound found no feasible solution.", scope = "OOQ Helpers", job_id = job_id)

  } else {
    flog.warn("OOQ status code is unknown", scope = "OOQ Helpers", job_id = job_id)
  }
  # Because Gurobi should remove any optimality gaps, we wish to send the problem
  # to Gurobi if lpSolve has not found a good solution or if the problem is infeasible
  if (!solver_status_code %in% c(0,2,9,12)) {
    flog.warn("LpSolve did not succeed with a problem. Sending the problem to Gurobi.", scope = "OOQ Helpers", job_id = job_id)
    return(list(solution_vector = NULL,
                rerun_problem_with_cloud_based_solver = should_rerun_problem_with_cloud_based_solver))
    }
}

#' Build OOQ Constraints for a Single Planning Market
#'
#' @param total_weeks The total number of weeks between the first in_shop_week_date of the region and the last in_shop_week_date + lifespan_in_weeks of the region.
#' @param lifespan_in_weeks The number of weeks ìn the lifespan of each PM.
#' @param week_to_start A numeric value
#' @param fc_lifespan_demand A numeric value
#' @param weekly_demand A numeric vector
#' @param service_level A numeric vector
#' @param ingoing_stock A numeric value
#' @param safety_stock A numeric vector
#' @param last_trail_timing A numeric value
#' @param max_UI_week_life A numeric value
#' @param max_number_of_trails A numeric value
#' @param min_ground_qty A numeric value
#' @param min_trail_qty A numeric value
#' @param maximum_ground_trail_quantity Maximum quantification value for the pm
#'
#' @return A list of the following elements:
#'         \itemize{
#'           \item{obj_vec}{A vector of length 3 * \code{total_weeks}}
#'           \item{constr_coeff_mat}{A matrix of size (4 * total_weeks + 4)
#'             x (3 * \code{total_weeks}) that stores all coefficients
#'             for the constraints}
#'           \item{constr_sign_vec}{A character vector of length (4 *
#'             total_weeks + 4) that represents types of constrains ("=",
#'             "<=", ">=")}
#'           \item{constr_value_vec}{A numeric vector of length (4 *
#'             total_weeks + 4) that represents RHS of constraints}
#'         }
#'
get_one_country_constraints <- function(
  total_weeks,
  lifespan_in_weeks,
  week_to_start,
  lead_time_weeks,
  week_to_start_adjusted,
  lifespan_in_weeks_adjusted,
  lifespan_range_adjusted,
  fc_lifespan_demand,
  weekly_demand,
  safety_stock,
  service_level,
  ingoing_stock,
  simulated_stock,
  safety_stock_buffer,
  last_trail_timing,
  max_UI_week_life,
  max_number_of_trails,
  min_ground_qty,
  min_trail_qty,
  maximum_ground_trail_quantity,
  def_orders_week,
  def_orders_quantity,
  sim_orders_week,
  def_orders_no_sim_week,
  emergency_orders_week,
  should_deactivate_minimum_trail
  ) {
  # initiate variables for storing results
  mat_list <- list()
  constr_sign_vec <- list()
  constr_value_vec <- list()
  obj_vec <- matrix(0, 1, total_weeks * 3)

  # all blocks of constraints will have the format
  #    [Oi|Qi|Si]
  # for i in 1:total_weeks
  # Where
  #   Oi - binary variable representing the decision to
  #        have an order in week
  #   Qi - real variables representing the quantity ordered
  #        in week
  #   Si - real variables representing the stock value in
  #        week

  # adjusting for lead time

  abs_last_trail_date <- min(lifespan_in_weeks_adjusted - last_trail_timing, max_UI_week_life - lead_time_weeks)
  contains_def_orders <- length(def_orders_week) > 0

  weekly_demand_adj <- weekly_demand[lead_time_weeks + 1:lifespan_in_weeks_adjusted]
  fc_lifespan_demand <- sum(weekly_demand_adj)

  safety_stock_buffer <- safety_stock_buffer[lifespan_range_adjusted]

  if (abs_last_trail_date < 0) {
    stop("An article has called OOQ with a lead time that is above the maximum trailing period")
  }


  ## Setting objective function ################################################
  # -- speed maximization -- currently no weight put on countries,
  # maybe add something to put more importance to bigger PMs
  # objective function is the average of inverse of speed
  # sum(Si / weekly_demand_adj[i]) * fc_lifespan_demand

  obj_vec[1, 2 * total_weeks + lifespan_range_adjusted] <- fc_lifespan_demand / weekly_demand_adj
  # additional weight is set on the last week of lifespan
  obj_vec[1, 2 * total_weeks + lifespan_in_weeks + week_to_start - 1] <- obj_vec[1, 2 * total_weeks + lifespan_in_weeks + week_to_start - 1] * (lifespan_in_weeks_adjusted - 1)

  ## building blocks for constraints
  null_mat <- matrix(0, total_weeks, total_weeks)
  null_vec <- matrix(0, 1, total_weeks * 3)
  diag_mat <- diag(1, total_weeks)

  ## Setting up constraint lhs, types and rhs of constraint blocks ##############
  # 1) constraint S1 = Q1 + ingoing stock till week_to_start_adjusted then Si = Si-1 - demand_i-1 + Qi;
  # Si = 0 < week_to_start_adjusted
  # -Qi + Si = ingoing stock for i = week_to_start_adjusted
  # -Qi - Si-1 + Si = -demand_i-1 for i > week_to_start_adjusted
  # constraint size: (lifespan_in_weeks_adjusted, total_weeks*3)

  si_vars <- diag_mat
  si_vars[which(row(si_vars) - 1 == col(si_vars) &
                    row(si_vars) > week_to_start_adjusted)] <- -1

  qi_vars <- null_mat
  qi_vars[which(row(si_vars) == col(si_vars) &
                    row(si_vars) >= week_to_start_adjusted)] <- -1

  # calculate constants
  c1 <- rep(0, total_weeks)
  c1[lifespan_range_adjusted] <- -lag(weekly_demand_adj, 1, default = 0)
  c1[week_to_start_adjusted] <- c1[week_to_start_adjusted] + ingoing_stock

  mat_list[[1]] <- cbind(null_mat,
                         qi_vars,
                         si_vars)

  constr_sign_vec <- c(constr_sign_vec, rep("=", total_weeks))
  constr_value_vec <- c(constr_value_vec, c1)

  # 2) constraints Qstart >= min_ground and Qi >= min_trail*Oi and Qi = 0 till week_to_start_adjusted
  # constraint size: (lifespan_in_weeks_adjusted, total_weeks*3)
  if (!should_deactivate_minimum_trail) {

    oi_vars <- -min_trail_qty * diag_mat

    c2 <- rep(0, total_weeks)

    if (week_to_start >= week_to_start_adjusted) {
      c2[week_to_start] <- min_ground_qty
    }

    if (contains_def_orders) {
      if (week_to_start %in% def_orders_week) { c2[week_to_start] <- 0 }

      oi_vars[col(oi_vars) == row(oi_vars)][def_orders_week] <- 0
    }
    oi_vars[col(oi_vars) == row(oi_vars)][week_to_start] <- 0

    mat_list[[2]] <- cbind(oi_vars,
                          diag_mat,
                          null_mat)[lifespan_range_adjusted,]

    constr_sign_vec <- c(constr_sign_vec, rep(">=", lifespan_in_weeks_adjusted))
    constr_value_vec <- c(constr_value_vec, c2[lifespan_range_adjusted])
  }

  # 3) constraints Qi <= maximum_ground_trail_quantity*Oi
  # const size: (lifespan_in_weeks_adjusted, total_weeks*3)

    # The maximum quantity should be at least max(safety_stock_buffer) + several time the lifespan demand.
    # The minimum regional constraint requires that we order a certain amount per region.
    # The minimum regional constraint could lead some pm to order more than their own pm lifespan demand in order to meet this regional constraint.
    # Therefore we allow some multiple of the pm lifespan demand as maximum quantity for long lifespan.
    # Enabling a pm to order a multiple of the pm lifespan demand can also lead to a better regional optimum.

  mat_list[[3]] <- cbind(-maximum_ground_trail_quantity * diag_mat,
                         diag_mat,
                         null_mat)

  constr_sign_vec <- c(constr_sign_vec, rep("<=", total_weeks))
  constr_value_vec <- c(constr_value_vec, rep(0, total_weeks))

  # 4) constraints O1 = 1 if week_to_start_adjusted > week_to_start
  # constraint size: (3, total_weeks*3)
  o1 <- null_vec

  # Take out the constraint if week_to_start_adjusted > week_to_start
  if (week_to_start_adjusted <= week_to_start) {
    o1[1, week_to_start] <- 1
    constr_value_vec <- c(constr_value_vec, 1)
  } else {
    constr_value_vec <- c(constr_value_vec, 0)
  }

  constr_sign_vec <- c(constr_sign_vec, "=")

  # constraints sum(Qi > fc_demand - ingoing stock)

  sum_qi <- kronecker(matrix(c(0, 1, 0), ncol = 3), t(c(rep(0, week_to_start_adjusted - 1), rep(1, total_weeks - week_to_start_adjusted + 1))))

  constr_sign_vec <- c(constr_sign_vec, ">=")
  constr_value_vec <- c(constr_value_vec, fc_lifespan_demand - ingoing_stock)

  # constraints sum(Oi) <= max number of trails
  sum_oi <- kronecker(matrix(c(1, 0, 0), ncol = 3), matrix(1, ncol = total_weeks))

  mat <- rbind(o1, sum_qi, sum_oi)

  mat_list[[4]] <- mat

  constr_sign_vec <- c(constr_sign_vec, "<=")

  # If min trail is deactivated, constraint ooq to only put one last order.
  if (!should_deactivate_minimum_trail) {
    constr_value_vec <- c(constr_value_vec, max_number_of_trails)
  } else {
    constr_value_vec <- c(constr_value_vec, length(def_orders_week) + 1)
  }

  ## 5) The safety stock constraint is Si >= safety_stock_buffer
  # constraint size: (lifespan_in_weeks_adjusted, total_weeks*3)

  c3 <- safety_stock_buffer

  i <- 0
  # We need to check which of the first week after lead time is blocked by a def order.
  while ((week_to_start_adjusted + i) %in% def_orders_no_sim_week) {
    # For each week after lead time that is a def order week,
    # we disable the safety stock constraint.
    c3[i + 1] <- 0
    i <- i + 1
  }

  mat_list[[5]] <- cbind(null_mat,
                        null_mat,
                        diag_mat)[lifespan_range_adjusted,]

  constr_sign_vec <- c(constr_sign_vec, rep(">=", lifespan_in_weeks_adjusted))
  constr_value_vec <- c(constr_value_vec, c3)


  ## 6) constraints 0i = 0 before start and 0i = 0 x weeks before end of lifespan
  # -- also no orders after UI window end (corresponding to a limit in Product
  # Plan client system for the season)
  # constraint size: (1, total_weeks*3)

  mat_1 <- null_vec
  mat_1[1, 1:total_weeks] <- 1
  mat_1[1, week_to_start_adjusted - 1 + 1:abs_last_trail_date] <- 0

  if (contains_def_orders) {
    mat_1[1, def_orders_week] <- rep(0, length(def_orders_week))
  }

  mat_list[[6]] <- mat_1
  constr_sign_vec <- c(constr_sign_vec, "=")
  constr_value_vec <- c(constr_value_vec, 0)

  # 7) Si / demand_i >= 1/max_s_percent
  # Update : The speed constraint is now merged with the safety stock constraint.
  # constraint size: (lifespan_in_weeks_adjusted, total_weeks*3)

  # 8) Def order constraint Q_i = Def_i and 0_i = 1
  # constraint size: (length(Def_orders_week), total_weeks*3)

  if (contains_def_orders) {
    mat_def_orders <- cbind(null_mat,
                           diag_mat,
                           null_mat)

    mat_list[[8]] <- mat_def_orders[def_orders_week,] # select only rows that correspond to def order weeks

    c5 <- def_orders_quantity

    constr_sign_vec <- c(constr_sign_vec, if_else((def_orders_week >= week_to_start_adjusted)
                                                  & (def_orders_week %in% sim_orders_week), '>=', '='))


    constr_value_vec <- c(constr_value_vec, c5)

    # 9) Def order constraint 0_i = 1 for a Def Order
    # constraint size: (length(Def_orders_week), total_weeks*3)

    mat_def_orders <- cbind(diag_mat,
                           null_mat,
                           null_mat)

    mat_list[[9]] <- mat_def_orders[def_orders_week,] # select only rows that correspond to def order weeks

    constr_sign_vec <- c(constr_sign_vec, rep("=", length(c5)))
    constr_value_vec <- c(constr_value_vec, rep(1, length(c5)))
  }

  # 10) constraints O_Emergency_week = 1
  # constraint size: (1, total_weeks*3)
  o10 <- null_vec

  # Take out the constraint if week_to_start_adjusted > week_to_start
  if (length(emergency_orders_week)) {
    o10[1, emergency_orders_week] <- 1
    constr_value_vec <- c(constr_value_vec, 1)
    mat_list[[10]] <- o10
    constr_sign_vec <- c(constr_sign_vec, "=")
  }



  ## Binding constraints together ##############################################
  constr_coeff_mat <- do.call(rbind, mat_list)
  constr_sign_vec <- unlist(constr_sign_vec)
  constr_value_vec <- unlist(constr_value_vec)

  return(list(obj_vec = obj_vec,
              constr_coeff_mat = constr_coeff_mat,
              constr_sign_vec = constr_sign_vec,
              constr_value_vec = constr_value_vec))
}

# Script containing every function for def order

########## Simulated stock ##########

#' Get a stock forecast only taking into account the quantity provided by def orders
#' and the ingoing stock and the weekly demand for a single planning market.
#' The general formula for this function is:
#'  Stock_i = max(0, Stock_{i-1} + Demand_{i-1}) + Def_Quantity_i
#'
#' @param total_weeks The total number of weeks between the first in_shop_week_date of the region and the last in_shop_week_date + lifespan_in_weeks of the region.
#' @param lead_time_weeks the number of weeks between `week_to_start` and `week_to_start_adjusted`.
#' @param def_orders_quantity The vector containing the quantity ordered for each def order.
#' @param def_orders_week The vector containing the week of the def order.
#' @param ingoing_stock The ingoing stock at week_to_start_adjusted.
#' @param week_to_start The in shop week.
#' @param week_to_start_adjusted The adjusted week to start quantification.
#' @param lifespan_in_weeks_adjusted The number of weeks ìn the lifespan of each PM starting from the end of lead time.
#' @param weekly_demand The demand for each week of the lifespan.
#'
#' @return A stock forecast with def orders only for a single planning market.
get_simulated_stock <- function(
  total_weeks,
  lead_time_weeks,
  def_orders_quantity,
  def_orders_week,
  ingoing_stock,
  week_to_start,
  week_to_start_adjusted,
  lifespan_in_weeks_adjusted,
  weekly_demand
  ) {

  # Initiate a simulated stock vector
  simulated_stock <- rep(0, total_weeks)
  contains_def_orders <- length(def_orders_quantity) > 0

  if (contains_def_orders) {
    # Initialization: s_0 = Ingoing_stock + def_orders_quantity_at_start
    simulated_stock[week_to_start_adjusted] <- ingoing_stock

    def_orders_quantity_at_start <- def_orders_quantity[def_orders_week == week_to_start_adjusted]
    if (length(def_orders_quantity_at_start) != 0) {
      simulated_stock[week_to_start_adjusted] <- simulated_stock[week_to_start_adjusted] + def_orders_quantity_at_start
    }

    for (i in week_to_start_adjusted + 1:(lifespan_in_weeks_adjusted - 1)) {

      # Stock_i = max(0, Stock_{i-1} + Demand_{i-1}) + Def_Quantity_i
      simulated_stock[i] <- max(0, simulated_stock[i - 1] - weekly_demand[i - week_to_start])

      def_orders_quantity_at_i <- def_orders_quantity[def_orders_week == i]
      if (length(def_orders_quantity_at_i) != 0) {
        simulated_stock[i] <- simulated_stock[i] + def_orders_quantity_at_i
      }
    }
  }

  return(simulated_stock)
}

########## Format Order ##########

#' Vectorize Orders week and quantity for a single planning market.
#'
#' @param orders A dplyr table containing orders information.
#' @param p An integer referencing the index of the for loop for pm.
#' @param article_id A list of article_id.
#' @param pm A list of pm.
#' @param start_date The first in shop week date of the region.
#' @param in_shop_week_date The in shop week date of `pm[p]`.
#' @param is_def_order_active A boolean informing us if we need to use def orders.
#'
#' @return A vector containing def orders week and a vector
#' containing def orders quantity for a single planning market.
format_order_table <- function(
  orders,
  p,
  article_id,
  pm,
  start_date,
  in_shop_week_date,
  is_def_order_active
  ) {

  orders_pm = orders[orders$article_id == article_id[p] & orders$pm == pm[p],] %>%
                                            filter(order_in_shop_week_date >= in_shop_week_date)

  # Check if def order for the pm is not an empty tibble due to no def orders or filtering.
  if (nrow(orders_pm) > 0 & is_def_order_active) {

    # Get the number of week since start_date. Indexing start from 1 for start_date
    weeks_since_start <- as.integer((orders_pm$order_in_shop_week_date - start_date) / DAYS_IN_WEEK) + 1

    orders_quantity <- orders_pm$quantity
    orders_week <- weeks_since_start

  } else {
    # We always need to return a list of def orders, so if there are none we return an empty list.
    orders_quantity <- list()
    orders_week <- list()
  }

  return(list(orders_quantity = orders_quantity, orders_week = orders_week))
}

########## Deactivate min trail ##########

#' We should deactivate min trail for one pm if:
#' - The stock margin is above `min_trail_lower_bound` for
#' the lifespan starting from `week_to_start_adjusted`.
#'
#' @param stock_margin The stock margin for the pm we want to check if we should deactivate min trail.
#' @param lifespan_range_adjusted A range from week_to_start_adjusted to week_to_start_adjusted + lifespan_in_weeks_adjusted.
#' @param min_trail_lower_bound The value under which we switch off min trail
#' @param order_lower_bound The value under which we switch off ooq.
#' @param pm The pm list.
#'
#' @return A boolean corresponding to the need of switching off min trail.
deactivate_min_trail <- function(
  stock_margin,
  lifespan_range_adjusted,
  min_trail_lower_bound,
  order_lower_bound,
  pm
  ) {

  # Select the weeks after `week_to_start_adjusted`
  stock_margin <- stock_margin[lifespan_range_adjusted]
  # If the stock margin is above `min_trail_lower_bound` for the whole lifespan we deactivate ooq.
  should_deactivate_minimum_trail <- (min(stock_margin) > min_trail_lower_bound
                        & min(stock_margin) < order_lower_bound)

  return(should_deactivate_minimum_trail)
}

########## Deactivate ooq ##########

#' As defined by the business requirements for def orders, we need to deactivate OOQ if any of the following are true.
#' We deactivate ooq for one pm if
#' - The stock margin is above `order_lower_bound` for the lifespan
#' starting from `week_to_start_adjusted`.
#' - The stock margin falls below zero in the last 4 week of the lifespan.
#' - The stock margin falls below zero but there is def orders until the last
#'  4 week of the lifespan.
#'
#' @param stock_margin The stock margin for the pm we want to check if we need to deactivate ooq.
#' @param lifespan_in_weeks_adjusted The number of weeks ìn the lifespan of each PM starting from the end of lead time.
#' @param week_to_start The in shop week date.
#' @param lifespan_range_adjusted A range from week_to_start_adjusted to week_to_start_adjusted + lifespan_in_weeks_adjusted.
#' @param week_to_start_adjusted The in shop week date adjusted.
#' @param def_orders_no_sim_week A list of def order with no sim on the same week.
#' @param order_lower_bound The value under which we switch off ooq.
#' @param pm The pm list.
#'
#' @return A boolean corresponding to the need of switching off ooq.
deactivate_ooq <- function(
  stock_margin,
  lifespan_in_weeks_adjusted,
  week_to_start,
  lifespan_range_adjusted,
  week_to_start_adjusted,
  def_orders_no_sim_week,
  order_lower_bound,
  pm
  ) {

  # Select the weeks after `week_to_start_adjusted`
  stock_margin <- stock_margin[lifespan_range_adjusted]

  # Get the weeks with stockout
  list_stockout <- which(stock_margin < 0)

  # Get the first stockout week
  first_stockout <- if_else(length(list_stockout) < 1, integer(1), list_stockout[1])

  # Check if:
  # - Min stock margin is above order_lower_bound.
  # - The first stockout is within the last 4 week of the lifespan.
  # - The first stockout can actually be dealt with or is it blocked
  #   by one or several def order until the last 4 weeks.
  should_deactivate_ooq <- (
                    min(stock_margin) > order_lower_bound
                    | first_stockout > (lifespan_in_weeks_adjusted - LIFESPAN_ENDING_INTERVAL)
                    | all((week_to_start_adjusted - 1 + first_stockout:(lifespan_in_weeks_adjusted - LIFESPAN_ENDING_INTERVAL)) %in% def_orders_no_sim_week)
                    )

  return(should_deactivate_ooq)
}


#' Simplified version of `get_one_country_constraint` to fix every suggested orders of OOQ
#' to be equal to def orders for a single planning market.
#'
#' @param total_weeks The total number of weeks between the first in_shop_week_date of the region and the last in_shop_week_date + lifespan_in_weeks of the region.
#' @param simulated_stock A simulated stock for the lifespan given def orders.
#' @param def_orders_week The def orders week vector for the pm.
#' @param def_orders_quantity The def orders quantity vector for the pm.
#'
#' @return A list of the following elements:
#'         \itemize{
#'           \item{obj_vec}{A vector of length 3 * \code{total_weeks}}
#'           \item{constr_coeff_mat}{A matrix of size (3 * total_weeks)
#'             x (3 * \code{total_weeks}) that stores all coefficients
#'             for the constraints}
#'           \item{constr_sign_vec}{A character vector of length (3 *
#'             total_weeks) filled with '='}
#'           \item{constr_value_vec}{A numeric vector of length (3 *
#'             total_weeks) that represents RHS of constraints}
#'         }
disabled_ooq_constraints <- function(
  total_weeks,
  simulated_stock,
  def_orders_week,
  def_orders_quantity
  ) {

  constraint_list <- list()
  constraint_sign_vector <- list()
  constraint_value_vector <- list()
  objective_vector <- matrix(0, 1, total_weeks * 3)
  contains_def_orders <- length(def_orders_week) > 0

  ## building blocks for constraints
  null_mat <- matrix(0, total_weeks, total_weeks)
  null_vec <- matrix(0, 1, total_weeks * 3)
  diag_mat <- diag(1, total_weeks)

  # Constraint fixing the value of stock to the value of simulated stock
  stock_constraint <- simulated_stock

  constraint_list[[1]] <- cbind(null_mat,
                         null_mat,
                         diag_mat)

  constraint_sign_vector <- c(constraint_sign_vector, rep("=", total_weeks))
  constraint_value_vector <- c(constraint_value_vector, stock_constraint)

  # Constraint fixing the value of quantity to the value of def orders quantities
  quantity_constraint <- rep(0, total_weeks)

  constraint_list[[2]] <- cbind(null_mat,
                          diag_mat,
                          null_mat)

  if (contains_def_orders) {
    quantity_constraint[def_orders_week] <- def_orders_quantity
  }

  constraint_sign_vector <- c(constraint_sign_vector, rep("=", total_weeks))
  constraint_value_vector <- c(constraint_value_vector, quantity_constraint)

  # Constraint fixing the week of orders to the week of def orders weeks
  order_constraints <- rep(0, total_weeks)

  constraint_list[[3]] <- cbind(diag_mat,
                          null_mat,
                          null_mat)

  if (contains_def_orders) {
    order_constraints[def_orders_week] <- 1
  }

  constraint_sign_vector <- c(constraint_sign_vector, rep("=", total_weeks))
  constraint_value_vector <- c(constraint_value_vector, order_constraints)

  ## Binding constraints together ##############################################
  constr_coeff_mat <- do.call(rbind, constraint_list)
  constr_sign_vec <- unlist(constraint_sign_vector)
  constr_value_vec <- unlist(constraint_value_vector)

  return(list(obj_vec = objective_vector, constr_coeff_mat = constr_coeff_mat, constr_sign_vec = constr_sign_vec, constr_value_vec = constr_value_vec))
}

########## Emergency week ##########

# Get the week for each pm that need to be relaxed from the link constraint.
#'
#' @param total_weeks The total number of weeks between the first in_shop_week_date of the region and the last in_shop_week_date + lifespan_in_weeks of the region.
#' @param number_of_countries The number of countries.
#' @param pm A list of pm.
#' @param lifespan_in_weeks The number of weeks ìn the lifespan of each PM.
#' @param week_to_start A vector of in shop week date per pm.
#' @param lead_time_weeks A vector of every number of week between in shop week date and end of lead time per pm.
#' @param week_to_start_adjusted A vector of in shop week date adjusted per pm.
#' @param lifespan_range_adjusted A range from week_to_start_adjusted to week_to_start_adjusted + lifespan_in_weeks_adjusted.
#' @param weekly_demand A list of weekly demand corresponding to demand throughout the lifespan per pm.
#' @param stock_margin A list of stock margin for the lifespan given def orders per pm.
#' @param def_orders_week A list of def order weeks per pm.
#' @param sim_orders_week A list of sim order weeks per pm.
#' @param min_weeks_btw_orders The minimum number of weeks between orders.
#' @param deactivated_ooq A list of boolean corresponding to deactivated pm.
#'
#' @return A list of integer corresponding to the week to unlock for each pm.
get_emergency_orders_week <- function(
  total_weeks,
  number_of_countries,
  pm,
  lifespan_in_weeks,
  week_to_start,
  lead_time_weeks,
  week_to_start_adjusted,
  lifespan_range_adjusted,
  weekly_demand,
  stock_margin,
  def_orders_week,
  sim_orders_week,
  min_weeks_between_orders,
  deactivated_ooq
  ) {

  # Initialize a memory of the result.
  emergency_orders_week <- list()
  past_emergency_orders_week <- list(1)

  # Get all the week with a def order on any pm
  total_def_orders_week = unlist(def_orders_week)

  # While the result differ from previous calculation we check
  # which week need to be blocked
  while (!identical(past_emergency_orders_week, emergency_orders_week)) {

    # Reset the memory
    past_emergency_orders_week <- emergency_orders_week

    # Get all the week with an emergency order on any pm
    total_emergency_orders_week = unique(unlist(emergency_orders_week))

    for (p in 1:number_of_countries) {

      # Get orders with sim order on the same week.
      def_with_sim_same_week <- intersect(def_orders_week[[p]], sim_orders_week[[p]])
      # Get orders without sim order on the same week.
      def_orders_no_sim_week <- setdiff(def_orders_week[[p]], sim_orders_week[[p]])
      # Get order only from other pm.
      total_def_other_pm_week <- setdiff(total_def_orders_week, def_orders_week[[p]])

      # Get an encoding of every blocked week.
      blocked_week <- get_blocked_week(def_orders_week[[p]],
                                      total_emergency_orders_week,
                                      def_with_sim_same_week,
                                      def_orders_no_sim_week,
                                      total_def_other_pm_week,
                                      week_to_start[p],
                                      week_to_start_adjusted[p],
                                      min_weeks_between_orders,
                                      total_weeks
                                      )

      # Get the first non blocked week
      first_non_blocked <- max(1, which(blocked_week == FALSE)[1], na.rm = TRUE)

      # Check if there is any safety stock issue between
      # `week_to_start_adjusted` and `first_non_blocked - 1`
      any_safety_stock_issue <- any(stock_margin[[p]][week_to_start_adjusted[p]:max(week_to_start_adjusted[p], first_non_blocked - 1)] < 0)

      # If we need to unlock weeks
      if (any_safety_stock_issue & !deactivated_ooq[p]) {

        emergency_week <- week_to_start_adjusted[p]

        # We need to check if the day to unlock is a def order week with no
        # sim order on the same week, if it is we pass it to the next week.
        emergency_week <- find_first_non_def_week(emergency_week, def_orders_no_sim_week)

        emergency_orders_week[[p]] <- emergency_week

      } else {

        emergency_orders_week[[p]] <- list()
      }
    }
  }

  return(emergency_orders_week)
}

#' Get the blocked week given def order and lead time for one pm
#'
#' @param def_orders_week A vector of def order.
#' @param total_emergency_orders_week Every emergency orders week.
#' @param def_with_sim_same_week A vector of def order with sim order weeks for the pm.
#' @param def_orders_no_sim_week A vector of def order weeks with no sim order vector for the pm.
#' @param total_def_orders_week Every def orders week.
#' @param week_to_start The in shop week date.
#' @param week_to_start_adjusted The in shop week date adjusted by the lead time.
#' @param min_weeks_btw_orders The minimum number of weeks between orders.
#' @param total_weeks The total number of weeks between the first in_shop_week_date of the region and the last in_shop_week_date + lifespan_in_weeks of the region.
#'
#' @return Get a vector of boolean corresponding to the blocked week.
get_blocked_week <- function(
  def_orders_week,
  total_emergency_orders_week,
  def_with_sim_same_week,
  def_orders_no_sim_week,
  total_def_other_pm_week,
  week_to_start,
  week_to_start_adjusted_p,
  min_weeks_between_orders,
  total_weeks
  ) {

  # Concatenate the week that are generating blocked week including the order week itself
  blocked_week_including_order_week <- unlist(c(def_orders_no_sim_week,
                                   week_to_start))

  # Encoding the blocked week into a vector.
  blocked_vec_including_order_week <- encoding_blocked_week_by_(blocked_week_including_order_week, TRUE, min_weeks_between_orders, total_weeks)

  # Concatenate the week that are generating blocked week not including the order week itself
  blocked_week_not_including_order_week <- unlist(c(total_def_other_pm_week,
                                        total_emergency_orders_week,
                                        def_with_sim_same_week))

  # Encoding the blocked week into a vector.
  blocked_vec_not_including_order_week <- encoding_blocked_week_by_(blocked_week_not_including_order_week, FALSE, min_weeks_between_orders, total_weeks)

  # Adding the blocked week together
  blocked_week <- blocked_vec_including_order_week + blocked_vec_not_including_order_week

  # Filter floating point error.
  blocked_week <- blocked_week > 1e-1

  # Unblock week with def order and sim order on the same week
  if (length(def_with_sim_same_week)) {
    blocked_week[def_with_sim_same_week] <- FALSE
  }

  # Block week prior to the lead time week
  blocked_week[1:max(1, week_to_start_adjusted_p - 1)] <- TRUE

  return(blocked_week)
}

#' Encode the blocked week given a vector.
#'
#' @param vector_week A list of order week that are blocking weeks.
#' @param is_order_week_blocked A boolean defining if we need
#'                              to consider the order week as a blocked week.
#' @param min_weeks_btw_orders The minimum number of weeks between orders.
#' @param total_weeks The total number of weeks between the first in_shop_week_date of the region and the last in_shop_week_date + lifespan_in_weeks of the region.
#'
#' @return an encoded vector with the blocked with as true.
encoding_blocked_week_by_ <- function(
  vector_week,
  is_order_week_blocked,
  min_weeks_between_orders,
  total_weeks
  ) {

  result_vec <- rep(0, total_weeks)

  # Encoding the order week into a vector
  if (length(vector_week)) {
    result_vec[vector_week] <- 1
  }

  # Define if we consider the order week as blocked or not blocked
  if (is_order_week_blocked) {
    filter <- c(rep(1, min_weeks_between_orders - 1), 1, rep(1, min_weeks_between_orders - 1))
  } else {
    filter <- c(rep(1, min_weeks_between_orders - 1), 0, rep(1, min_weeks_between_orders - 1))
  }

  # Encoding the blocked week into a vector using the filter
  result_with_padding <- convolve(result_vec, filter, type = "open")
  result_vec <- result_with_padding[min_weeks_between_orders - 1 + 1:total_weeks]

  return(result_vec)
}

#' Find the first week that is not a def order with no sim order on the same week
#'
#' @param safety_week The week we want to unlock to fulfil safety stock constraint.
#' @param def_orders_no_sim_week The def order vector for the PM
#'
#' @return the week with no sim order on the same week
find_first_non_def_week <- function(safety_week, def_orders_no_sim_week) {
  i <- safety_week
  while (i %in% def_orders_no_sim_week) {
    i <- i + 1
  }

  return(i)
}

########## Link constraint relaxation ##########

#' Remove link constraint between two lists of weeks.
#'
#' The link constraint is enforced with the following equation:
#' \forall t, \forall pmi,
#'    O_{t, pmi} + O_{t+1, pmi} + O_{t+2, pmi} + sum_{pmj} O_{t+3,pmj}}/number_of_countries <= 1
#'
#' Now because of def order, we will have to enforce some order to be placed on some
#' week no matter what.
#'
#' Let's take an example.
#' Let's say we have one def order at time t* and in the planning market pm*
#'   We would have to enforce: O_{t*, pmi*} = 1
#'
#' In that case, this is going to impact `min_weeks_between_orders` rows of the link constraint.
#' In this example let's take 4.
#'
#' O_{t*, pmi} + O_{t*+1, pmi} + O_{t*+2, pmi} + sum_{pmj} O_{t*+3,pmj}}/number_of_countries <= 1
#' O_{t*-1, pmi} + O_{t*, pmi} + O_{t*+1, pmi} + sum_{pmj} O_{t*+2,pmj}}/number_of_countries <= 1
#' O_{t*-2, pmi} + O_{t*-1, pmi} + O_{t*, pmi} + sum_{pmj} O_{t*+1,pmj}}/number_of_countries <= 1
#' O_{t*-3, pmi} + O_{t*-2, pmi} + O_{t*-1, pmi} + sum_{pmj} O_{t*,pmj}}/number_of_countries <= 1
#'
#' As O_{t*, pmi*} = 1, those equations can be simplified as:
#'    O_{t*+1, pmi} + O_{t*+2, pmi} + sum_{pmj} O_{t*+3,pmj}}/number_of_countries = 0
#'    O_{t*-1, pmi} + O_{t*+1, pmi} + sum_{pmj} O_{t*+2,pmj}}/number_of_countries = 0
#'    O_{t*-2, pmi} + O_{t*-1, pmi} + sum_{pmj} O_{t*+1,pmj}}/number_of_countries = 0
#'    O_{t*-3, pmi} + O_{t*-2, pmi} + O_{t*-1, pmi} + sum_{pmj!=pmi} O_{t*,pmj}}/number_of_countries <= 1-1/number_of_countries
#'
#' Implying:
#' O_{t*-3, pmi} = 0,
#' O_{t*-2, pmi} = 0,
#' O_{t*-1, pmi} = 0,
#' \forall pmj, O_{t*+1, pmj} = 0
#' \forall pmj, O_{t*+2, pmj} = 0
#' \forall pmj, O_{t*+3, pmj} = 0
#'
#' This might not always be possible because:
#' - We can have a stockout due to those implications.
#' - We can have a def order and have to enforce one of those implication above.
#'
#' In those case we need to relax the link constraint.
#' We will change the coefficient 1 in to a 0 in front of the week we need to block.
#'
#' Let's say we want to place an order on {t*+2, pmi}.
#'
#' We have to change the equation to be:
#' 0*O_{t*, pmi} + O_{t*+1, pmi} + O_{t*+2, pmi} + sum_{pmj} O_{t*+3,pmj}}/number_of_countries <= 1
#' O_{t*-1, pmi} + 0*O_{t, pmi} + O_{t*+1, pmi} + sum_{pmj} O_{t*+2,pmj}}/number_of_countries <= 1
#'
#' We will have to modify `min_weeks_between_orders - week_difference` number of rows.
#'
#' Now let's take the another example where we have to place a def_order on {t*+2, pmj}.
#'
#' O_{t*-1, pmi} + 0*O_{t, pmi} + O_{t*+1, pmi} + sum_{pmj} O_{t*+2,pmj}}/number_of_countries <= 1
#'
#' This is the logic of the link constraint relaxation.
#'
#'
#' @param order_week_list_1 A list of list, containing weeks that
#'                           needs to be separated by `min_weeks_between_orders`
#'                           from the weeks in `order_week_list_2`.
#' @param order_week_list_2 A list of list, containing weeks that
#'                           needs to be separated by `min_weeks_between_orders`
#'                           from the weeks in `order_week_list_1`.
#' @param link_constraint_matrix The link_constraint_matrix with
#'                               the constraint matrix modeling the links
#' @param number_of_countries The number of countries.
#' @param rows_per_country The number of rows per country
#' @param vars_per_country The number of vars per country
#' @param min_weeks_between_orders The split time between orders
#'
#' @return Get a link_constraint_matrix with no conflicting rows
remove_link_constraint <- function(
  order_week_list_1,
  order_week_list_2,
  link_constraint_matrix,
  number_of_countries,
  rows_per_country,
  vars_per_country,
  min_weeks_between_orders
  ) {

  # Check for every pair of country (i, j) in number_of_countries^2
  for (i in 1:number_of_countries) {
    for (j in 1:number_of_countries) {

      # If there is list of element of i in order_week_list_1 and j in order_week_list_2
      if (length(order_week_list_1[[i]]) > 0 & length(order_week_list_2[[j]]) > 0) {

        # Loop over each element
        for (event_week_i in order_week_list_1[[i]]) {
          for (event_week_j in order_week_list_2[[j]]) {

            # Get the difference for each pair of element.
            week_difference = abs(event_week_i - event_week_j)

            # If the difference in weeks is above 0 and below `min_weeks_between_orders` change constraints
            if (week_difference > 0 & week_difference < min_weeks_between_orders) {

              # Get the min and the max week
              min_pair <- min(event_week_i, event_week_j)
              max_pair <- max(event_week_i, event_week_j)

              # if i and j are equal, we need to remove the constraint
              # within one pm as stated in the description.
              if (i == j) {

                columns_to_delete <- vars_per_country * (i - 1) + min_pair
                rows_to_delete <- pmin(rows_per_country * (i), pmax(rows_per_country * (i - 1),
                          rows_per_country * (i - 1) + min_pair + min_weeks_between_orders - 1:(min_weeks_between_orders - week_difference)))
                link_constraint_matrix[rows_to_delete, columns_to_delete] <- 0

              } else {

                # If i and j are not equal we need to remove the
                # constraint between pm as stated in the description.
                pm_min <- c(i, j)[which.min(c(event_week_i, event_week_j))]
                pm_max <- c(i, j)[which.max(c(event_week_i, event_week_j))]

                # The constraint changed is the diagonal in
                # Mat_link[pm_min, pm_max] at the column max_pair
                link_constraint_matrix[rows_per_country * (pm_min - 1) + max_pair,
                           vars_per_country * (pm_max - 1) + max_pair] <- 0
              }
            }
          }
        }
      }
    }
  }
  return(link_constraint_matrix)
}

#' Getting the average speed from OOQ output.
#'
#' @param n_countries The number of countries.
#' @param weekly_demand The list of weekly demand from wdf per pm.
#' @param lead_time_weeks The lead time week per pm.
#' @param lifespan_w_adjusted The lifespan starting from end of lead time week per pm.
#' @param stock_matrix The stock matrix from OOQ output.
#' @param index_adjusted The list of index from end of lead time week to end of lifespan per pm.
#'
#' Return the average speed for each pm.
get_average_speed <- function(
  n_countries,
  weekly_demand,
  lead_time_weeks,
  lifespan_w_adjusted,
  stock_matrix,
  index_adjusted
  ) {
  average_speed <- c()
  for (i in 1:n_countries){
    # Getting the demand from the end of lead time to the end of lifespan
    demand_after_lead_time <- weekly_demand[[i]][lead_time_weeks[i]+1:lifespan_w_adjusted[i]]
    # Getting the stock from the end of lead time to the end of lifespan
    stock_after_lead_time <- stock_matrix[i,index_adjusted[[i]]]
    speed <- demand_after_lead_time/stock_after_lead_time
    # Calculating the average speed if there is any non infinite value
    if (any(is.finite(speed))) {
      mean_speed <- mean(speed*is.finite(speed), na.rm=TRUE)
    } else {
      mean_speed <- 0
    }
    average_speed <- c(average_speed, mean_speed)
  }
  return(average_speed)
}

#' Calculate reasonable maximum to reduce the search space and avoid OOQ to output crazy value.
#'
#' @param number_of_countries Number of countries.
#' @param minimum_ground_qty Minimum ground quantity per pm.
#' @param minimum_trail_qty Minimum trail quantity per pm.
#' @param minimum_reg_trail_qty Minimum regional trail quantity per pm.
#' @param def_orders_quantity List containing the quantity ordered for each def order.
#' @param safety_stock_buffer List containing the safety stock buffer vector for each def order.
#'                            This represent the minimum stock we should have for each week
#' @param total_lifespan_demand Vector containing the total demand for the lifespan for each pm.
#' @param quantification_lifespan_tolerance User defined parameter corresponding to the tolerance
#'                                          for the maximum quantification as a multiplier of a demand lifespan.
#' @param job_id Job ID to be used for logging.
#'
#' @return Maximum ground trail quantity as a vector representing each pm.
get_maximum_ground_trail_quantity <- function(
  number_of_countries,
  minimum_ground_qty,
  minimum_trail_qty,
  minimum_reg_trail_qty,
  def_orders_quantity,
  safety_stock_buffer,
  total_lifespan_demand,
  quantification_lifespan_tolerance,
  job_id
  ) {

  maximum_ground_trail_quantity <- c()

  for (i in 1:number_of_countries) {

    # Maximum quantity should be above any minimum constraint to avoid infeasibility
    max_of_minimum_constraint <- max(
      minimum_ground_qty[i],
      minimum_trail_qty[i],
      minimum_reg_trail_qty,
      unlist(def_orders_quantity[[i]]),
      safety_stock_buffer[[i]]
    )

    # Calculate minimum ground ratio compared to the
    # lifespan demand to flag too high minimum ground.
    ratio_minimum_ground_lifespan_demand <- as.integer(
      minimum_ground_qty[i] / total_lifespan_demand[i]
    )

    if (ratio_minimum_ground_lifespan_demand > 5) {
      flog.warn(
        paste(
          "One of the minimum ground quantity corresponds to",
          ratio_minimum_ground_lifespan_demand,
          "the lifespan demand"
        ),
        scope="OOQ Helpers",
        job_id = job_id
      )
    }

    # Calculate minimum trail ratio compared to the
    # lifespan demand to flag too high minimum trail.
    ratio_minimum_trail_lifespan_demand <- as.integer(
      minimum_trail_qty[i] / total_lifespan_demand[i]
    )

    if (ratio_minimum_trail_lifespan_demand > 5) {
      flog.warn(
        paste(
          "The maximum safety stock value corresponds to",
          ratio_max_safety_stock,
          "the lifespan demand"
        ),
        scope="OOQ Helpers",
        job_id = job_id
      )
    }

    # Calculate maximum safety stock ratio compared to the
    # lifespan demand to flag too high safety stock value.
    ratio_max_safety_stock <- as.integer(
      max(safety_stock_buffer[[i]]) / total_lifespan_demand[i]
    )

    if (ratio_max_safety_stock > 5) {
      flog.warn(
        paste(
          "The maximum safety stock value corresponds to",
          ratio_max_safety_stock,
          "the lifespan demand"
        ),
        scope="OOQ Helpers",
        job_id = job_id
      )
    }

    maximum_ground_trail_quantity[i] <- max_of_minimum_constraint + total_lifespan_demand[i] * (quantification_lifespan_tolerance + 1)
  }

  return(maximum_ground_trail_quantity)
}


#' Determine if we want to solve with cloud-based solver.
#'
#' @param number_countries Total number of planning markets in the problem instance.
#' @param total_lifespan_weeks Integer showing total lifespan in weeks.
#'
#' @return Boolean flag denoting if we will send the problem to the cloud (TRUE) or not (FALSE).
should_solve_with_cloud_based_solver <- function(
  number_countries,
  total_lifespan_weeks
) {
  complexity_factor <- number_countries * total_lifespan_weeks
  if (
    complexity_factor >= ENVIRONMENT_VARIABLES$OOQ_COMPLEXITY_THRESHOLD
    | total_lifespan_weeks >= ENVIRONMENT_VARIABLES$OOQ_LIFESPAN_THRESHOLD
  ){
    return(TRUE)
  }
  else {
    return(FALSE)
  }
}


#' Solve a defined problem using Gurobi
#'
#' @param number_countries Total number of PM's in the problem instance
#' @param total_lifespan_weeks Integer showing total lifespan in weeks
#' @param job_id Job ID used for flogging
#' @param constr_coeff_matr Constraint coefficient matrix
#' @param obj_vec Values of objective function
#' @param constr_value_vec Values of constraints
#' @param constr_sign_vec Sign of constraints e.g. >=
#' @param params List of optional parameters to pass to Gurobi
#'
#' @return Solution vector showing the value for each decision variable
solve_with_cloud_based_solver <- function(
  number_countries,
  total_lifespan_weeks,
  job_id,
  constr_coeff_mat,
  obj_vec,
  constr_value_vec,
  constr_sign_vec,
  params
) {
  flog.info("Solving with Gurobi", scope = "OOQ Helpers", job_id = job_id)
  # The input naming of variables in Gurobi is different than in lpSolveAPI.
  gurobi_var_types <- rep(c("B", "C", "C"), each = total_lifespan_weeks) %>%
    rep(times = number_countries)

  # Build model object with input parameters to solve problem.
  model <- list()
  model$A <- constr_coeff_mat
  model$obj        <- obj_vec
  model$modelsense <- 'min'
  model$rhs        <- constr_value_vec
  model$sense      <- constr_sign_vec
  model$vtype      <- gurobi_var_types

  gurobi_result <- gurobi(model, params)

  flog.info(paste(
    'Gurobi has reached solution status:',
    gurobi_result$status), scope = "OOQ Helpers", job_id = job_id
  )
  flog.info(paste(
    'Solution:',
    gurobi_result$objval), scope = "OOQ Helpers", job_id = job_id
  )
  solution_vector <- gurobi_result$x
  return(solution_vector)
}
