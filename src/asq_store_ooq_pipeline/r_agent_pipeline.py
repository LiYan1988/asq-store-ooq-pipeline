import os
import subprocess
import shutil
import pathlib


class RAgentPipeline:
    ASSORTMENT_URL = 'https://HM-GROUP@dev.azure.com/HM-GROUP/BT-AI-Foundation-ASQ-Store-Algo/_git/assortment'
    OOQ_URL = 'https://HM-GROUP@dev.azure.com/HM-GROUP/BT-AI-Foundation-ASQ-Store-Algo/_git/asq-store-ooq'

    R_TEMPLATE_PATH = pathlib.Path(__file__).parent.parent.parent / 'resources'

    def __init__(self, main_path: os.PathLike, python_path: os.PathLike):
        self.main_path = main_path
        self.assortment_path = self.main_path / 'assortment'

        self.ooq_path = self.assortment_path / 'r_agent/r_agent/modules_python/asq-store-ooq'

        self.python_path = python_path
        self.server_r_path = self.assortment_path / 'r_agent/r_agent/server.R'
        self.wrapper_ooq_r_path = self.assortment_path / 'r_agent/r_agent/wrappers/wrapper_ooq.R'

    def clone_repos(self):
        cmd_clone_assortment = f'git clone -b develop {self.ASSORTMENT_URL} {self.assortment_path}'
        os.makedirs(self.assortment_path, exist_ok=True)
        subprocess.run(cmd_clone_assortment.split())

        cmd_clone_ooq = f'git clone -b develop {self.OOQ_URL} {self.ooq_path}'
        os.makedirs(self.ooq_path, exist_ok=True)
        subprocess.run(cmd_clone_ooq.split())

    def copy_r_files(self):
        shutil.copyfile(
            self.R_TEMPLATE_PATH / 'server.R',
            self.server_r_path
        )
        os.makedirs(self.wrapper_ooq_r_path.parent, exist_ok=True)
        shutil.copyfile(
            self.R_TEMPLATE_PATH / 'wrapper_ooq.R',
            self.wrapper_ooq_r_path
        )
        shutil.copyfile(
            self.R_TEMPLATE_PATH / 'module_ooq.R',
            self.assortment_path / 'r_agent/r_agent/modules/module_ooq.R'
        )
        shutil.copyfile(
            self.R_TEMPLATE_PATH / 'module_ooq_helpers.R',
            self.assortment_path / 'r_agent/r_agent/modules/module_ooq_helpers.R'
        )

    def _modify_server_r(self, binary_input_directory: os.PathLike, binary_input_filename: str):
        working_directory = self.assortment_path / 'r_agent'
        binary_input_path = binary_input_directory / binary_input_filename

        with open(self.server_r_path, 'r') as fp:
            lines = fp.readlines()

        lines[8] = f'setwd("{working_directory}")\n'
        lines[71] = f'PYTHON_PATH = "{self.python_path}"\n'
        lines[77] = f'filename = "{binary_input_path}"\n'

        with open(self.server_r_path, 'w') as fp:
            fp.writelines(lines)

    def _modify_wrapper_ooq_r(self, output_directory: os.PathLike, output_directory_basename: str):
        save_path = output_directory / output_directory_basename
        os.makedirs(save_path, exist_ok=True)
        with open(self.wrapper_ooq_r_path, 'r') as fp:
            lines = fp.readlines()

        lines[177] = f'    base_dump_path = "{str(save_path)}/"\n'
        with open(self.wrapper_ooq_r_path, 'w') as fp:
            fp.writelines(lines)

    def source_server_r(
            self,
            binary_input_directory: os.PathLike,
            binary_input_filename: str,
            output_directory: os.PathLike,
            output_directory_basename: str,
            r_out_directory: os.PathLike,
            r_out_filename: str
    ):
        self._modify_server_r(binary_input_directory, binary_input_filename)
        self._modify_wrapper_ooq_r(output_directory, output_directory_basename)
        cmd = f'R CMD BATCH {self.server_r_path}'
        subprocess.run(cmd.split())
        if os.path.isfile('server.Rout'):
            os.makedirs(r_out_directory, exist_ok=True)
            shutil.move('server.Rout', r_out_directory / r_out_filename)


if __name__ == '__main__':
    main_path = pathlib.Path('/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-5/')
    python_path = pathlib.Path('/Users/liynx/anaconda3/envs/asq-store-ooq-1261/bin/python')
    r_agent_pipeline = RAgentPipeline(main_path=main_path, python_path=python_path)

    r_agent_pipeline.clone_repos()
    r_agent_pipeline.copy_r_files()

    binary_input_directory = pathlib.Path('/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/data/binaries')
    binary_input_filename = 'ApiRequests_202105222309.csv-1.data'
    output_directory = main_path / 'r_agent_output'
    job_id = 'dummy_job_id'
    r_out_directory = main_path / 'server_r_out'
    r_out_filename = f'{job_id}.Rout'

    r_agent_pipeline.source_server_r(
        binary_input_directory,
        binary_input_filename,
        output_directory,
        job_id,
        r_out_directory,
        r_out_filename
    )

