import asyncio
import logging
import os
import time
from typing import List, Optional

from pydantic import Field
from web3 import Web3

from inputs.base import Message, SensorConfig
from inputs.base.loop import FuserInput
from providers.io_provider import IOProvider


class WalletEthereumConfig(SensorConfig):
    """
    Configuration for Ethereum Wallet Sensor.

    Parameters
    ----------
    provider_url : str
        The Ethereum provider URL.
    poll_interval : float
        Interval in seconds between blockchain data updates.
    account_address : str
        The Ethereum account address to monitor.
    """

    provider_url: str = Field(
        default="https://eth.llamarpc.com", description="The Ethereum provider URL."
    )
    poll_interval: float = Field(
        default=4.0, description="Interval in seconds between blockchain data updates."
    )
    account_address: str = Field(
        default_factory=lambda: os.environ.get(
            "ETH_ADDRESS", "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        ),
        description="The Ethereum account address to monitor.",
    )


class WalletEthereum(FuserInput[WalletEthereumConfig, List[float]]):
    """
    Ethereum wallet monitor that tracks ETH balance changes.

    Queries the Ethereum blockchain for account balance updates and reports
    incoming transactions.

    Raises
    ------
    Exception
        If connection to Ethereum network fails
    """

    def __init__(self, config: WalletEthereumConfig):
        """
        Initialize WalletEthereum instance.
        """
        super().__init__(config)

        # Track IO
        self.io_provider = IOProvider()

        self.ETH_balance = 0.0
        self.ETH_balance_previous = 0.0
        self.balance_eth = 0.0
        self.balance_change = 0.0

        self.messages: list[Message] = []
        self.eth_info = {}

        self.PROVIDER_URL = self.config.provider_url
        self.POLL_INTERVAL = self.config.poll_interval
        self.ACCOUNT_ADDRESS = self.config.account_address
        logging.debug(f"Using {self.ACCOUNT_ADDRESS} as the wallet address")
        logging.info("Testing: WalletEthereum: Initialized")

        # Initialize Web3
        self.web3 = Web3(Web3.HTTPProvider(self.PROVIDER_URL))
        if not self.web3.is_connected():
            raise Exception("Failed to connect to Ethereum")

    def _get_balance_blocking(self):
        """
        Blocking call to get Ethereum balance.
        """
        try:
            block_number = self.web3.eth.block_number
            balance_wei = self.web3.eth.get_balance(self.ACCOUNT_ADDRESS)  # type: ignore
            balance_eth = float(self.web3.from_wei(balance_wei, "ether"))
            return block_number, balance_eth
        except Exception as e:
            logging.error(f"Error fetching blockchain data: {e}")
            return None, None

    async def _poll(self) -> List[float]:
        """
        Poll for Ethereum balance updates.

        Returns
        -------
        List[float]
            [current_balance, balance_change]
        """
        await asyncio.sleep(self.POLL_INTERVAL)

        loop = asyncio.get_running_loop()
        block_number, balance_eth = await loop.run_in_executor(
            None, self._get_balance_blocking
        )

        if block_number is not None and balance_eth is not None:
            self.balance_eth = balance_eth
            self.eth_info = {
                "block_number": int(block_number),
                "address": str(self.ACCOUNT_ADDRESS),
                "balance": self.balance_eth,
            }
            logging.debug(
                f"Block: {self.eth_info['block_number']}, Account Balance: {self.eth_info['balance']:.3f} ETH"
            )

            self.ETH_balance = self.balance_eth
            self.balance_change = self.ETH_balance - self.ETH_balance_previous
            self.ETH_balance_previous = self.ETH_balance

        return [self.ETH_balance, self.balance_change]

    async def _raw_to_text(self, raw_input: List[float]) -> Optional[Message]:
        """
        Convert balance data to human-readable message.

        Parameters
        ----------
        raw_input : List[float]
            [current_balance, balance_change]

        Returns
        -------
        Optional[Message]
            Timestamped status or transaction notification
        """
        # balance = raw_input[0]
        balance_change = raw_input[1]

        if balance_change > 0:
            message = f"You just received {balance_change:.3f} ETH."
            logging.debug(f"WalletEthereum: {message}")
            return Message(timestamp=time.time(), message=message)

        return None

    async def raw_to_text(self, raw_input: List[float]):
        """
        Process balance update and manage message buffer.

        Parameters
        ----------
        raw_input : List[float]
            Raw balance data
        """
        pending_message = await self._raw_to_text(raw_input)

        if pending_message is not None:
            self.messages.append(pending_message)

    def formatted_latest_buffer(self) -> Optional[str]:
        """
        Format and clear the latest buffer contents.

        Returns
        -------
        Optional[str]
            Formatted string of buffer contents or None if buffer is empty
        """
        if len(self.messages) == 0:
            return None

        latest_message = self.messages[-1]

        result = f"""
{self.__class__.__name__} INPUT
// START
{latest_message.message}
// END
"""

        self.io_provider.add_input(
            self.__class__.__name__, latest_message.message, latest_message.timestamp
        )
        self.messages = []
        return result
