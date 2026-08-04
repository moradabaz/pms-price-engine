from unittest.mock import patch

from dbt_runner.main import run_dbt_command, run_once
from dbt_runner.settings import DbtRunnerSettings


def test_run_dbt_command_returns_true_on_success():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = ""

        assert run_dbt_command(["run"]) is True
        mock_run.assert_called_once_with(["dbt", "run"], capture_output=True, text=True)


def test_run_dbt_command_returns_false_on_failure():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = "some output"
        mock_run.return_value.stderr = "some error"

        assert run_dbt_command(["test"]) is False


def test_run_once_invokes_seed_run_test_with_project_and_profiles_dirs():
    settings = DbtRunnerSettings(project_dir="/proj", profiles_dir="/prof")

    with patch("dbt_runner.main.run_dbt_command", return_value=True) as mock_cmd:
        results = run_once(settings)

    assert results == {"seed": True, "run": True, "test": True}
    expected_dirs = ["--project-dir", "/proj", "--profiles-dir", "/prof"]
    mock_cmd.assert_any_call(["seed", *expected_dirs])
    mock_cmd.assert_any_call(["run", *expected_dirs])
    mock_cmd.assert_any_call(["test", *expected_dirs])
