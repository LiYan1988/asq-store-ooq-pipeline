"""Entrypoint for PyOOQ.

Orchestrates optimization of order quantities (OOQ) for multiple regions.
"""

# Import system libraries.
import logging
import os
import sys
from typing import List, Tuple, Union
import shutil
import pathlib
import json

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
import ooq.run_ooq as process_ooq_job

# Configure logging.
os.chdir(pathlib.Path(__file__).parent)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
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
        should_plot_solution: bool,
) -> Tuple[pd.DataFrame, List[int], pd.DataFrame, pd.DataFrame]:

    (ooq_solution, pms_with_enough_stock,
     pm_level_params, week_level_params) = process_ooq_job.execute(
        weekly_seasonless_demand, orders_for_target, planning_market_features,
        forecast_store_numbers, ooq_parameters, ingoing_stock, should_plot_solution)

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

