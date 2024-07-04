import argparse
import logging
import multiprocessing as mp
from pipelines import PipelineManager

def run_pipeline_wrapper(pipeline_name, pipeline_manager):
    pipeline_manager.run_pipeline(pipeline_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data pipelines.")
    parser.add_argument("--run", help="Run a specific pipeline by name")
    args = parser.parse_args()

    pipeline_manager = PipelineManager('Pipelines', 'Schedules', 'Connections', 'Emails')

    if args.run:
        num_cores = mp.cpu_count() - 1
        with mp.Pool(num_cores) as pool:
            pool.starmap(run_pipeline_wrapper, [(args.run, pipeline_manager)])
    else:
        logging.info("No pipeline specified to run. Use --run <pipeline_name> to run a specific pipeline.")
