# Flame executor

import signal

from entropylab.flame.execute import _utils as execute_utils
from entropylab.flame.execute._config import logger
from entropylab.flame.execute._execute import Execute


def _main():
    logger.info("Execute. Start")
    signal.signal(signal.SIGINT, execute_utils.exit_gracefully)
    signal.signal(signal.SIGTERM, execute_utils.exit_gracefully)
    args = execute_utils.parse_args()
    execute_ = Execute(args)
    msg = execute_.run()
    print(msg)
    logger.info("Execute. Finish")


if __name__ == "__main__":
    _main()
