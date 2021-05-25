import os
import pathlib
import subprocess
import shutil


class OOQPipeline:
    def __init__(self, ooq_path: os.PathLike, work_dir: os.PathLike):
        self.ooq_path = ooq_path
        self.resources_path = (pathlib.Path(__file__).parent.parent.parent / 'resources').resolve()
        self.work_dir = work_dir.resolve()

    def install_ooq(self, copy_requirements=False):
        if copy_requirements:
            shutil.copyfile(self.resources_path / 'requirements.txt', self.ooq_path / 'requirements.txt')
        cmd = f'pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -e {self.ooq_path}'
        subprocess.run(cmd.split())

    def create_script(
            self,
            job_id: str,
            experiment_name: str,
            input_csv_path: os.PathLike,
            output_path: os.PathLike,
    ):
        shutil.copyfile(self.resources_path / 'run_ooq.py', self.work_dir / 'run_ooq.py')
        with open(self.work_dir / 'run_ooq.py', 'r') as fp:
            lines = fp.readlines()

        # Here I assume that the input csv files are saved in this way
        input_csv_path = str(input_csv_path / job_id)
        output_path = str(output_path / f'{job_id}/{experiment_name}')
        lines[323] = f"    input_csv_path = pathlib.Path('{input_csv_path}')\n"
        lines[325] = f"    output_path = pathlib.Path('{output_path}')\n"

        with open(self.work_dir / 'run_ooq.py', 'w') as fp:
            fp.writelines(lines)

    def run_ooq(self):
        script_path = str(self.work_dir / 'run_ooq.py')
        cmd = f'python {script_path}'
        subprocess.run(cmd.split())
        os.remove(self.work_dir / 'run_ooq.py')

if __name__ == '__main__':
    ooq_path = pathlib.Path('/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-5/assortment/r_agent/r_agent/modules_python/asq-store-ooq')
    work_dir = pathlib.Path('/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-5/assortment/r_agent/r_agent/modules_python/asq-store-ooq')
    ooq_pipeline = OOQPipeline(ooq_path, work_dir)
    ooq_pipeline.install_ooq()

    ooq_pipeline.create_script(
        job_id='FF9BCC7E-3B83-EB11-B566-0050F2443C80',
        experiment_name='base',
        input_csv_path=pathlib.Path('/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-5/r_agent_output/'),
        output_path=pathlib.Path('/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-5/ooq_output/')
    )

    ooq_pipeline.run_ooq()
