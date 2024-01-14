
import logging
import subprocess
from collections.abc import Callable

from project.utils.file_system import load_yaml
from project.utils.thread import SpinnerThread, ThreadWithReturnValue


class JupyterPipeline:
    def __init__(self, use_active_venv: bool = False, note_book_listing_path: str = "project/config/analysation_scripts.yaml") -> None:
        self._use_active_venv = use_active_venv
        self._notebook_paths = load_yaml(note_book_listing_path)

    def run(self):
        """Start pipeline"""
        poetry_prefix = 'poetry run ' if not self._use_active_venv else ''
        
        # load scripts to execute
        for notebook in self._notebook_paths:
            print("[NbClientApp] Executing ", notebook)
            cmd = f"{poetry_prefix}jupyter execute --allow-errors {notebook}"

            jupyter_thread = ThreadWithReturnValue(target=self._create_subprocess_func(cmd))
            jupyter_thread.start()

            spinner_thread = SpinnerThread()
            spinner_thread.start()

            result = jupyter_thread.join()
            spinner_thread.stop()

            stderr = result.stderr.decode("utf-8")
            
            if "FileNotFoundError:" in stderr:
                msg = f"No such file or directory: '{notebook}'"
                logging.error(msg)
            elif (
                "nbclient.exceptions.CellExecutionError: An error occurred while executing the following cell:"
                in stderr
            ):
                msg = "Error while executing the notebook: " + stderr.split("\n")[-3]
                logging.error(msg)
            elif "poetry: command not found" in stderr:
                logging.error(
                    """
                poetry command was not found. 
                Please either:
                    * ensure that poetry is visible and all packages are installed
                    * or if you want to proceed without poetry, create a venv with all dependencies, activate it, and repeat this command with the flag --use-active-venv true
                """
                )
            else:
                logging.error(stderr)

    def _create_subprocess_func(self, command: str) -> Callable:
        def inner() -> object:
            result = subprocess.run(command, shell=True, capture_output=True, check=False)
            return result
        
        return inner