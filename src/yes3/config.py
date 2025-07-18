import logging
from dataclasses import asdict, dataclass

from boto3.s3.transfer import TransferConfig

from yes3.utils.logs import check_level, get_logger

PROGRESS_MODES = {'off', 'all', 'large'}


@dataclass
class YeS3Config:
    default_region: str = 'us-east-2'
    log_level: int = logging.WARNING
    progress_mode: str = 'large'
    progress_size_threshold: int | float = 100e6  # bytes
    upload_transfer_config: TransferConfig | None = None

    @staticmethod
    def check_progress_mode(value) -> str:
        if value is None:
            value = 'off'
        if not isinstance(value, str):
            raise TypeError(f'progress_mode must be a str with one of the following values: {PROGRESS_MODES}')
        elif value.lower() not in PROGRESS_MODES:
            raise ValueError(f"Invalid progress_mode '{value}', must be one of {PROGRESS_MODES}")
        return value.lower()

    def __setattr__(self, name, value):
        if name == 'progress_mode':
            value = self.check_progress_mode(value)
        if name == 'log_level':
            root_logger = get_logger()
            value = check_level(value)
            if value != root_logger.level:
                root_logger.setLevel(value)
        super().__setattr__(name, value)

    def __repr__(self):
        params = {k: v for k, v in asdict(self).items() if v is not None}
        params_str = ', '.join(f'{k}={v!r}' for k, v in params.items())
        return f'{type(self).__name__}({params_str})'
