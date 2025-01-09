from typing import Optional, Self

import boto3
from botocore.client import BaseClient
from botocore.session import Session


class ClientContainer:
    special_attrs = {'special_attrs', 'configure', 'client', 'session'}

    def __init__(self):
        self.client: Optional[BaseClient] = None
        self.session: Optional[Session] = None

    def configure(
        self,
        client: Optional[BaseClient] = None,
        region: Optional[str] = None,
        profile: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
    ) -> Self:
        if client is not None:
            self.client = client
        else:
            sess_kwargs = {}
            client_kwargs = {}
            if profile:
                sess_kwargs['profile_name'] = profile
            if access_key:
                sess_kwargs['aws_access_key_id'] = access_key
            if secret_access_key:
                sess_kwargs['aws_secret_access_key'] = secret_access_key
            if session_token:
                sess_kwargs['aws_session_token'] = session_token
            if region:
                client_kwargs['region_name'] = region
            if sess_kwargs:
                self.session = boto3.Session(**sess_kwargs)
                self.client = self.session.client('s3', **client_kwargs)
            else:
                self.client = boto3.client('s3', **client_kwargs)
        return self

    def __getattribute__(self, name: str):
        if name in type(self).special_attrs:
            return super().__getattribute__(name)
        else:
            if self.client is None:
                self.configure()
            return getattr(self.client, name)


_client = ClientContainer()


def configure_client(
        client: Optional[BaseClient] = None,
        region: Optional[str] = None,
        profile: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
) -> ClientContainer:
    return _client.configure(
        client=client,
        region=region,
        profile=profile,
        access_key=access_key,
        secret_access_key=secret_access_key,
        session_token=session_token
    )


def get_client() -> ClientContainer:
    return _client


def set_client(client: ClientContainer):
    global _client
    _client = client
