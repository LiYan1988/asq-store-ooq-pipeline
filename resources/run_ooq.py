"""Entrypoint for PyOOQ.

Orchestrates optimization of order quantities (OOQ) for multiple regions.
"""

# Import system libraries.
import logging
import os
import shutil
import sys
from typing import List, Tuple, Union
import json
import pathlib

# Import third party libraries.
from multiprocessing.pool import ThreadPool
import pandas as pd

# NOTE:Below hack is only required when running PyOOQ from Reticulate.
# TODO: Package and install PyOOQ in the R Agent.
sys.path.append("/app/r_agent/r_agent/modules_python/asq-store-ooq")

# Import custom libraries.
import ooq.data_processing.data_processing_helpers as data_process_helpers
import ooq.utils.visualization.visualization as plotting
from ooq.data_processing.data_processing import process_input_data
from ooq.optimization.helpers import (
    core_helpers,
    def_orders_helpers,
    logging_helpers,
)
from ooq.regional_solver import run_ooq_for_one_region

# Configure logging.
os.chdir(pathlib.Path(__file__).parent)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"), filename='ooq.log')
logger = logging.getLogger(__name__)
logger.info(f"Current working directory: {os.getcwd()}.")
logger.info(f"Python Path: {sys.path}.")

def run_python_ooq(
        weekly_seasonless_demand: pd.DataFrame,
        orders_for_target: pd.DataFrame,
        planning_market_features: pd.DataFrame,
        forecast_store_numbers: pd.DataFrame,
        ooq_parameters: pd.DataFrame,
        ingoing_stock: pd.DataFrame,
        should_plot_solution: bool
) -> Tuple[pd.DataFrame, List[Union[int, str]], pd.DataFrame, pd.DataFrame]:
    """Run the OOQ optimization module: 'how much to buy when'.

    Each region is solved as a separate problem.
    This function orchestrates the execution of all regions,
    and returns a combined solution.

    :param weekly_seasonless_demand: Seasonles demand in target season.
    :param orders_for_target: All existing orders (no matter type) in target.
    :param planning_market_features: Planning market - PAS code information.
    :param forecast_store_numbers: Store estimate by planning market in target.
    :param ooq_parameters: Parameters to tweak OOQ behavior.
    :param ingoing_stock: Ingoing stock in the in-shop week
                            or leadtime week by planning market.
    :param should_plot_solution: Indicates if visualization is required.
    :return: Suggested orders by week and planning market, for all regions.
    """
    (
        weekly_seasonless_demand,
        orders_for_target,
        planning_market_features,
        forecast_store_numbers,
        ooq_parameters,
        ingoing_stock,
        region_to_planning_market_map,
        order_timing_parameters,
        first_in_shop_week,
        planning_markets,
    ) = process_input_data(
        weekly_seasonless_demand,
        orders_for_target,
        planning_market_features,
        forecast_store_numbers,
        ooq_parameters,
        ingoing_stock,
    )

    # Determine if def-order logic needs to be activated.
    is_def_orders_activated = def_orders_helpers.check_if_def_orders_are_activated(
        orders_for_target, planning_market_features
    )

    # If there are no planning markets to quantify, we can abort the
    # quantification here.
    if not planning_markets:
        logger.warning(
            "No planning markets to quantify. Returning no"
            " order suggestions."
        )
        return data_process_helpers.build_no_quantification_needed_output()

    # Build demand-related parameters. The full lifespan demand is
    # needed for the safety stock calculations and minimum quantities.
    # The lead time adjusted demand is used for the objective function.
    (
        demand_forecast,
        lifespan_demand,
        max_forecast_error,
        lead_time_adjusted_demand,
        lead_time_adjusted_lifespan_demand,
    ) = data_process_helpers.prepare_demand_values(
        weekly_seasonless_demand,
        planning_market_features,
        order_timing_parameters,
        region_to_planning_market_map,
        first_in_shop_week,
    )

    # Prepare safety stock parameters.
    safety_stock_parameters = data_process_helpers.prepare_safety_stock_input(
        forecast_store_numbers,
        ooq_parameters,
        planning_market_features,
        lifespan_demand,
    )

    # Build quantity-related parameters e.g. the safety stock buffer.
    # All of these variables are used when modelling the constraints.
    (
        safety_stock_buffer,
        minimum_ground_and_trail_quantity,
        maximum_ground_trail_quantity,
        minimum_regional_trail_quantity,
        initial_safety_stock,
        safety_stock
    ) = core_helpers.build_quantity_parameters(
        demand_forecast,
        safety_stock_parameters,
        planning_market_features,
        ooq_parameters,
        lifespan_demand,
        max_forecast_error,
        lead_time_adjusted_demand,
        orders_for_target,
    )

    # Build parameters used for def order logic.
    (
        planning_markets,
        should_deactivate_ooq,
        def_orders_data,
        projected_stock_above_safety_stock,
        projected_stock_level,
        region_to_planning_market_map,
        pms_with_enough_stock,
    ) = def_orders_helpers.build_def_orders_parameters(
        orders_for_target,
        first_in_shop_week,
        region_to_planning_market_map,
        ingoing_stock,
        demand_forecast,
        order_timing_parameters,
        planning_markets,
        safety_stock_buffer,
        is_def_orders_activated,
    )

    # Because the deactivated planning markets do not have to be solved,
    # we check if there are any remaining planning markets to optimize
    # for. If not, return no order suggestions.
    if not planning_markets:
        logger.warning(
            "No planning markets to quantify after "
            "removing planning markets that have a too "
            "high projected stock. Returning empty order suggestion."
        )
        return data_process_helpers.build_no_quantification_needed_output()

    # Start looping through each region and solve the OOQ module.
    unique_regions = region_to_planning_market_map["region_id"].unique()

    # Creating data to pass pas_code information on to the timing.py module
    # This was needed to extract the season & year of an article
    pas_code_info = pd.merge(
        left=forecast_store_numbers,
        right=region_to_planning_market_map,
        how="inner",
        on="planning_market_id"
    )
    pas_code_info=pas_code_info.rename(
        columns={
            "region_id":"region"
        },
    )

    # Multithreading requires the input data to be iterable. Thus, we
    # pack the input data in a tuple and repeat it as many times as we
    # have regions.
    data_for_ooq = []
    for region in unique_regions:
        data_for_ooq.append(
            (
                region,
                region_to_planning_market_map,
                demand_forecast,
                lead_time_adjusted_demand,
                lead_time_adjusted_lifespan_demand,
                safety_stock_buffer,
                minimum_ground_and_trail_quantity,
                minimum_regional_trail_quantity,
                maximum_ground_trail_quantity,
                def_orders_data,
                projected_stock_above_safety_stock,
                order_timing_parameters,
                initial_safety_stock,
                first_in_shop_week,
                ingoing_stock,
                is_def_orders_activated,
                pas_code_info
            )
        )

    # Multithread the OOQ problem of each region.
    # Uncomment the starmap code for easier local debug after commenting out the threadpool code
    logger.info("Starting the multithread with nano")
    # from itertools import starmap
    # ooq_result = list(starmap(run_ooq_for_one_region, data_for_ooq))

    with ThreadPool(len(unique_regions)) as p:
        ooq_result = p.starmap(run_ooq_for_one_region, data_for_ooq)

    # Initialize an empty dataframe to add the suggested orders to.
    ooq_solution = pd.DataFrame()
    ooq_stock_level = pd.DataFrame()

    # The ooq_result is now a tuple with [N regions][2] dimensions. We
    # append the solution to the solution and stock level dataframes.
    for region in range(len(unique_regions)):
        ooq_solution = pd.concat([ooq_solution, ooq_result[region][0]])
        ooq_stock_level = pd.concat(
            [ooq_stock_level, ooq_result[region][1]], axis=1
        )

    # Append the OOQ solution with the def orders of a deactivated
    # planning market if a planning market has been deactivated.
    if any(should_deactivate_ooq):
        ooq_solution = def_orders_helpers.append_deactivated_ooq_def_orders_to_solution(
            def_orders_data, should_deactivate_ooq, ooq_solution
        )

    # Plotting if desired.
    if should_plot_solution:
        for region in unique_regions:
            # Prepare plotting data.
            (
                def_orders_formatted,
                ooq_sol_formatted,
                ooq_stock_level_formatted,
                safety_stock_buffer_formatted,
                x_axis_dates,
            ) = plotting.format_data_for_plotting(
                def_orders_data,
                ooq_solution,
                should_deactivate_ooq,
                order_timing_parameters,
                projected_stock_level,
                ooq_stock_level,
                region,
                region_to_planning_market_map,
                safety_stock_buffer,
                first_in_shop_week,
            )

            # Visualize the optimal solution.
            plotting.plot_ooq_solution(
                def_orders_formatted,
                ooq_sol_formatted,
                safety_stock_buffer_formatted,
                ooq_stock_level_formatted,
                demand_forecast,
                minimum_ground_and_trail_quantity,
                region_to_planning_market_map,
                order_timing_parameters,
                should_deactivate_ooq,
                region,
                x_axis_dates
            )

    week_level_params = logging_helpers.create_week_level_params_empty_trace()
    pm_level_params = logging_helpers.create_pm_level_params_empty_trace()

    # Get the solution in the wanted output format.
    if not ooq_solution.empty:
        ooq_solution = core_helpers.format_ooq_solution(
            ooq_solution,
            forecast_store_numbers,
            order_timing_parameters,
            orders_for_target,
        )

        week_level_params = logging_helpers.create_week_level_params_trace(
            forecast_store_numbers,
            lead_time_adjusted_demand,
            safety_stock_buffer,
            safety_stock
        )

        pm_level_params = logging_helpers.create_pm_level_params_trace(
            ooq_parameters,
            forecast_store_numbers,
            minimum_regional_trail_quantity,
            minimum_ground_and_trail_quantity,
            planning_market_features,
            safety_stock_parameters,
        )

    return (
        ooq_solution,
        pms_with_enough_stock,
        pm_level_params,
        week_level_params,
    )

if __name__ == '__main__':
    # Set input parameters
    input_csv_path = pathlib.Path('/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-5/r_agent_output/FF9BCC7E-3B83-EB11-B566-0050F2443C80')  # job id
    # Set output parameters
    output_path = pathlib.Path('/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-5/ooq_output/FF9BCC7E-3B83-EB11-B566-0050F2443C80/base')
    data_output_path = output_path / 'data'
    figure_output_path = output_path / 'figure'
    os.makedirs(data_output_path, exist_ok=True)
    os.makedirs(figure_output_path, exist_ok=True)

    # Load input data
    weekly_seasonless_demand = pd.read_csv(input_csv_path / "weekly_seasonless_demand.csv")
    orders_for_target = pd.read_csv(input_csv_path / "orders_for_target.csv")
    planning_market_features = pd.read_csv(
        input_csv_path / "planning_market_features_for_target.csv"
    )
    forecast_store_numbers = pd.read_csv(
        input_csv_path / "forecasted_store_number_for_target.csv"
    )
    ooq_parameters = pd.read_csv(input_csv_path / "ooq_parameters.csv")
    ingoing_stock = pd.read_csv(input_csv_path / "ingoing_stock.csv")

    # Temporary path to saved figures
    plot_path_origin = pathlib.Path(__file__).parent / 'output/plots'
    os.makedirs(plot_path_origin, exist_ok=True)

    # Run ooq
    ooq_solution, pms_with_enough_stock, pm_level_params, week_level_params = run_python_ooq(
        weekly_seasonless_demand,
        orders_for_target,
        planning_market_features,
        forecast_store_numbers,
        ooq_parameters,
        ingoing_stock,
        should_plot_solution=True
    )

    # Save output data
    ooq_solution.to_csv(data_output_path / 'ooq_solution.csv', sep=";", index=0)
    pms_with_enough_stock_path = data_output_path / 'pms_with_enough_stock.json'
    with open(pms_with_enough_stock_path, 'w') as fp:
        json.dump(pms_with_enough_stock, fp)
    pm_level_params.to_csv(data_output_path / 'pm_level_params.csv', sep=";", index=0)
    week_level_params.to_csv(data_output_path / 'week_level_params.csv', sep=";", index=0)

    # Copy figures
    for filename in os.listdir(plot_path_origin):
        shutil.copyfile(plot_path_origin / filename, figure_output_path / filename)
        (plot_path_origin / filename).unlink(missing_ok=True)
    # Remove the temporary figure directory
    plot_path_origin.rmdir()
    plot_path_origin.parent.rmdir()

    # Copy log file
    shutil.copyfile('ooq.log', output_path / 'ooq.log')
    os.remove('ooq.log')

    print('Done')

