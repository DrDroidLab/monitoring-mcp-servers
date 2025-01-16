import io
import logging
import subprocess

import paramiko

from integrations.processor import Processor

logger = logging.getLogger(__name__)


class BashProcessor(Processor):
    client = None

    def __init__(self, remote_host=None, remote_password=None, remote_pem=None, port=None):
        if remote_host:
            self.remote_user = remote_host.split("@")[0]
            self.remote_host = remote_host.split("@")[1]
        self.remote_password = remote_password
        self.remote_pem = remote_pem.strip()
        self.port = port if port else 22

    def get_connection(self):
        try:
            client = paramiko.SSHClient()
            error_string = ''
            client_inputs = {}
            if self.remote_pem:
                try:
                    if self.remote_password:
                        key = paramiko.RSAKey.from_private_key(io.StringIO(self.remote_pem),
                                                               password=self.remote_password)
                    else:
                        key = paramiko.RSAKey.from_private_key(io.StringIO(self.remote_pem))
                except Exception as e:
                    error_string += f"Error with RSA key type: {str(e)}\n"
                    try:
                        if self.remote_password:
                            key = paramiko.Ed25519Key.from_private_key(io.StringIO(self.remote_pem),
                                                                       password=self.remote_password)
                        else:
                            key = paramiko.Ed25519Key.from_private_key(io.StringIO(self.remote_pem))
                    except Exception as e:
                        error_string += f"Error with Ed25519 key type: {str(e)}\n"
                        try:
                            if self.remote_password:
                                key = paramiko.ECDSAKey.from_private_key(
                                    io.StringIO(self.remote_pem), password=self.remote_password)
                            else:
                                key = paramiko.ECDSAKey.from_private_key(io.StringIO(self.remote_pem))
                        except Exception as e:
                            error_string += f"Error with ECDSA key type: {str(e)}\n"
                            try:
                                if self.remote_password:
                                    key = paramiko.DSSKey.from_private_key(
                                        io.StringIO(self.remote_pem), password=self.remote_password)
                                else:
                                    key = paramiko.DSSKey.from_private_key(io.StringIO(self.remote_pem))
                            except Exception as e:
                                error_string += f"Error with DSS key type: {str(e)}\n"
                                logger.error(f"BashProcessor.get_connection:: Exception occurred while creating remote "
                                             f"connection with all types of supported key types: {error_string}")
                                raise Exception(f"Error with all types of supported key types: {error_string}")
                client_inputs['pkey'] = key
            if self.remote_host:
                client_inputs['hostname'] = self.remote_host
            if self.remote_user:
                client_inputs['username'] = self.remote_user
            if self.remote_password:
                client_inputs['password'] = self.remote_password
            if self.port:
                client_inputs['port'] = self.port
            if client_inputs:
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(**client_inputs)
            else:
                client = None
            return client
        except Exception as e:
            logger.error(f"Exception occurred while creating remote connection with error: {e}")
            raise e

    def test_connection(self):
        try:
            command = 'echo "Connection successful"'
            client = self.get_connection()
            if client:
                try:
                    stdin, stdout, stderr = client.exec_command(command)
                    output = stdout.read().decode('utf-8')
                    if output.strip() == "Connection successful":
                        return True
                    else:
                        raise Exception("Connection failed")
                except paramiko.AuthenticationException as e:
                    logger.error(f"Authentication error: {str(e)}")
                    raise e
                except paramiko.SSHException as e:
                    logger.error(f"SSH connection error: {str(e)}")
                    raise e
                except Exception as e:
                    logger.error(f"Error: {str(e)}")
                    raise e
                finally:
                    client.close()
            else:
                logger.info("Executing bash command locally")
                try:
                    result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE, universal_newlines=True)
                    return True if result.stdout.strip() == "Connection successful" else False
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error executing command{command}: {e}")
                    raise e
        except Exception as e:
            logger.error(f"Exception occurred while creating remote connection with error: {e}")
            raise e

    def execute_command(self, command):
        try:
            client = self.get_connection()
            if client:
                try:
                    stdin, stdout, stderr = client.exec_command(command)
                    output = stdout.read().decode('utf-8')
                    return output.strip()
                except paramiko.AuthenticationException as e:
                    logger.error(f"Authentication error: {str(e)}")
                    raise e
                except paramiko.SSHException as e:
                    logger.error(f"SSH connection error: {str(e)}")
                    raise e
                except Exception as e:
                    logger.error(f"Error: {str(e)}")
                    raise e
                finally:
                    client.close()
            else:
                logger.info("Executing bash command locally")
                try:
                    result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            universal_newlines=True)
                    return result.stdout.strip()
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error executing command{command}: {e}")
                    raise e
        except Exception as e:
            logger.error(f"Exception occurred while executing remote command with error: {e}")
            raise e
