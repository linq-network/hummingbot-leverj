import time

from web3.auto import w3
from eth_account.messages import defunct_hash_message


class LeverjPerpetualAuth:
    def __init__(self, api_key: str, account_id: str, secret_key: str):
        self._api_key = api_key
        self._account_id = account_id
        self._secret_key = secret_key

    @property
    def account_id(self):
        return self._account_id

    @property
    def api_key(self):
        return self._api_key
    
    @property
    def secret_key(self):
        return self._secret_key

    def generate_nonce(self):
        return int(time.time()*1000)

    def to_vrs(self, signature):
        signature_hex = signature[2:]
        r = self.bytes_to_hexstring(bytes.fromhex(signature_hex[0:64]))
        s = self.bytes_to_hexstring(bytes.fromhex(signature_hex[64:128]))
        v = ord(bytes.fromhex(signature_hex[128:130]))

        return v, r, s

    def bytes_to_hexstring(self, value):
        if isinstance(value, bytes) or isinstance(value, bytearray):
            return "0x" + "".join(map(lambda b: format(b, "02x"), value))
        elif isinstance(value, str):
            b = bytearray()
            b.extend(map(ord, value))
            return "0x" + "".join(map(lambda b: format(b, "02x"), b))
        else:
            raise AssertionError

    def sign(self, data):
        data = defunct_hash_message(primitive=bytes(data, 'utf-8'))
        signed_message = w3.eth.account.signHash(data, self._secret_key)
        return signed_message.signature.hex()

    def generate_request_headers(self, method, url, headers = None, body = None, params = None):
        nonce = self.generate_nonce()
        signature = self.sign(str(nonce))
        v, r, s = self.to_vrs(signature)

        auth_header = f"NONCE {self._account_id}.{self._api_key}.{v}.{r}.{s}"

        request_headers = {
            "Authorization": auth_header,
            "Nonce": str(nonce),
            "Content-Type": "application/json"
        }

        request_headers.update(headers)

        return request_headers

