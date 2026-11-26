#!/usr/bin/env python3

import click
import json
import os

from yes3 import s3


@click.command()
@click.argument('url')
@click.option('--pretty/--no-pretty', is_flag=True, default=True, help='Pretty print the JSON output')
@click.option('--indent', type=int, default=4, help='Indentation level for pretty printing')
def main(url: str, pretty: bool, indent: int):
    """
    Read a JSON file from an S3 URL and print it to the console.
    """
    if s3.is_s3_url(url):
        json_data = s3.read(url, file_type='json')
    elif os.path.exists(url):
        with open(url, 'r') as f:
            json_data = json.load(f)
    else:
        raise ValueError(f"JSON file not found: {url!r}")

    # Print the JSON data
    if pretty:
        print(json.dumps(json_data, indent=indent))
    else:
        print(json_data)


if __name__ == '__main__':
    main()
