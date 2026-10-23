import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    'native_trials', Path(__file__).with_name('repeat-native-tests.py'))
trials = importlib.util.module_from_spec(spec)
spec.loader.exec_module(trials)


class NativeTrialEvidenceTests(unittest.TestCase):
    def test_zero_exit_does_not_hide_conflicts_or_empty_test_selection(self):
        for output, qualified in [
            ('test result: ok. 1 passed;\n', True),
            ('test result: ok. 0 passed;\n', False),
            ('failed to realize committed handoffs\ntest result: ok. 1 passed;\n', False),
            ('rewrite rejected: previous transcript revision\ntest result: ok. 1 passed;\n', False),
        ]:
            with self.subTest(output=output), tempfile.TemporaryDirectory() as directory:
                result = subprocess.CompletedProcess([], 0, output, 'runtime diagnostics\n')
                with patch.object(trials.subprocess, 'run', return_value=result) as run:
                    row = trials.qualify_retirement('/fixture/test', Path(directory), 1)
                self.assertEqual(row['qualified'], qualified)
                self.assertEqual((Path(directory) / 'attempt-01.log').read_text(),
                                 output + 'runtime diagnostics\n')
                self.assertEqual(json.loads((Path(directory) / 'attempt-01.json').read_text()), row)
                self.assertEqual(run.call_args.kwargs['env']['FUGUE_ISOLATED_RUNTIME_TEST'],
                                 'runtime_members_emit_canonical_structural_events')

    def test_timeout_retains_last_observed_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            timeout = subprocess.TimeoutExpired([], 180, b'archive in flight\n', b'last diagnostic\n')
            with patch.object(trials.subprocess, 'run', side_effect=timeout):
                row = trials.qualify_retirement('/fixture/test', Path(directory), 2)
            self.assertFalse(row['qualified'])
            self.assertTrue(row['timeout'])
            self.assertEqual((Path(directory) / 'attempt-02.log').read_text(),
                             'archive in flight\nlast diagnostic\n')


if __name__ == '__main__':
    unittest.main()
