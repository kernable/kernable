"""Python SDK: a kernel on this machine is retried within two seconds after it went away (a
restart is over in seconds); one elsewhere backs off to 30 s.

Run from sdks/python: .venv/bin/python -m unittest tests.unit.test_reconnect
"""

import unittest

from kernable import _is_local


class LocalKernels(unittest.TestCase):
    def test_kernels_on_this_machine_are_local(self):
        for url in ["unix:/srv/run/kernel.sock", "http://127.0.0.1:7233", "127.0.0.1:7233",
                    "http://localhost:7233", "http://[::1]:7233", "http://127.0.0.1:7233,http://127.0.0.1:7235"]:
            self.assertTrue(_is_local(url), url)
        for url in ["https://kernel.example.com:7233", "http://10.0.0.5:7233",
                    "http://127.0.0.1:7233,https://b.example:7233", "http://localhost.example.com:7233"]:
            self.assertFalse(_is_local(url), url)


if __name__ == "__main__":
    unittest.main()
