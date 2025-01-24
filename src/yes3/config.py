from dataclasses import dataclass

PROGRESS_MODES = {'off', 'all', 'large'}


@dataclass
class YeS3Config:
    default_region: str = 'us-east-2'
    verbose: bool = False
    progress_mode: str = 'large'
    progress_size: int | float = 50e6  # bytes

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
        super().__setattr__(name, value)
