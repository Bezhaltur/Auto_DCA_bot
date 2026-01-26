"""
ERC20 USDT operations for EVM chains.
Handles balance checks, approvals, and transfers.
"""

import logging
from typing import Optional, Tuple
from web3 import Web3
from web3.exceptions import ContractLogicError, TransactionNotFound
from eth_account import Account
from networks import get_network_config, get_usdt_contract_address
from test_config import DRY_RUN, mask_sensitive_data

logger = logging.getLogger(__name__)

# ERC20 ABI (minimal - only functions we need)
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"}
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_to", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    }
]


def get_web3_instance(network_key: str) -> Web3:
    """
    Create Web3 instance for network.
    
    Args:
        network_key: Network key (e.g., "USDT-ARB")
    
    Returns:
        Web3 instance
    """
    config = get_network_config(network_key)
    w3 = Web3(Web3.HTTPProvider(config["rpc_url"]))
    
    if not w3.is_connected():
        raise RuntimeError(f"Failed to connect to {config['name']} RPC: {config['rpc_url']}")
    
    return w3


def get_usdt_contract(w3: Web3, network_key: str):
    """Get USDT contract instance."""
    contract_address = get_usdt_contract_address(network_key)
    return w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=ERC20_ABI)


def get_usdt_balance(w3: Web3, network_key: str, address: str) -> float:
    """
    Get USDT balance for address.
    
    Args:
        w3: Web3 instance
        network_key: Network key
        address: Wallet address
    
    Returns:
        USDT balance (float)
    """
    try:
        contract = get_usdt_contract(w3, network_key)
        decimals = contract.functions.decimals().call()
        balance_wei = contract.functions.balanceOf(Web3.to_checksum_address(address)).call()
        balance = balance_wei / (10 ** decimals)
        masked_addr = f"{address[:6]}...{address[-4:]}" if len(address) > 10 else address
        logger.info(f"Balance check: {masked_addr} on {network_key} = {balance:.6f} USDT")
        return balance
    except Exception as e:
        masked_addr = f"{address[:6]}...{address[-4:]}" if len(address) > 10 else address
        logger.error(f"Error getting USDT balance for {masked_addr} on {network_key}: {e}")
        raise RuntimeError(f"Failed to get USDT balance: {e}")


def get_native_balance(w3: Web3, address: str) -> float:
    """
    Get native token balance (ETH/BNB/MATIC).
    
    Args:
        w3: Web3 instance
        address: Wallet address
    
    Returns:
        Native token balance in ETH/BNB/MATIC
    """
    try:
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(address))
        balance = w3.from_wei(balance_wei, "ether")
        masked_addr = f"{address[:6]}...{address[-4:]}" if len(address) > 10 else address
        logger.info(f"Native balance check: {masked_addr} = {float(balance):.6f}")
        return float(balance)
    except Exception as e:
        masked_addr = f"{address[:6]}...{address[-4:]}" if len(address) > 10 else address
        logger.error(f"Error getting native balance for {masked_addr}: {e}")
        raise RuntimeError(f"Failed to get native balance: {e}")


def estimate_gas_for_approve(w3: Web3, network_key: str, from_address: str, spender_address: str, amount: float) -> int:
    """
    Estimate gas for approve transaction.
    
    Args:
        w3: Web3 instance
        network_key: Network key
        from_address: Address approving
        spender_address: Address being approved
        amount: Amount to approve
    
    Returns:
        Estimated gas (int)
    """
    try:
        contract = get_usdt_contract(w3, network_key)
        decimals = contract.functions.decimals().call()
        amount_wei = int(amount * (10 ** decimals))
        
        tx = contract.functions.approve(
            Web3.to_checksum_address(spender_address),
            amount_wei
        ).build_transaction({
            "from": Web3.to_checksum_address(from_address),
            "nonce": w3.eth.get_transaction_count(Web3.to_checksum_address(from_address)),
        })
        
        estimated_gas = w3.eth.estimate_gas(tx)
        masked_from = f"{from_address[:6]}...{from_address[-4:]}" if len(from_address) > 10 else from_address
        masked_spender = f"{spender_address[:6]}...{spender_address[-4:]}" if len(spender_address) > 10 else spender_address
        logger.info(f"Gas estimation (approve): {network_key}, from={masked_from}, spender={masked_spender}, amount={amount:.6f} USDT, gas={estimated_gas}")
        return estimated_gas
    except Exception as e:
        logger.error(f"Error estimating gas for approve: {e}")
        raise RuntimeError(f"Failed to estimate gas for approve: {e}")


def estimate_gas_for_transfer(w3: Web3, network_key: str, from_address: str, to_address: str, amount: float) -> int:
    """
    Estimate gas for transfer transaction.
    
    Args:
        w3: Web3 instance
        network_key: Network key
        from_address: Sender address
        to_address: Recipient address
        amount: Amount to transfer
    
    Returns:
        Estimated gas (int)
    """
    try:
        contract = get_usdt_contract(w3, network_key)
        decimals = contract.functions.decimals().call()
        amount_wei = int(amount * (10 ** decimals))
        
        tx = contract.functions.transfer(
            Web3.to_checksum_address(to_address),
            amount_wei
        ).build_transaction({
            "from": Web3.to_checksum_address(from_address),
            "nonce": w3.eth.get_transaction_count(Web3.to_checksum_address(from_address)),
        })
        
        estimated_gas = w3.eth.estimate_gas(tx)
        masked_from = f"{from_address[:6]}...{from_address[-4:]}" if len(from_address) > 10 else from_address
        masked_to = f"{to_address[:6]}...{to_address[-4:]}" if len(to_address) > 10 else to_address
        logger.info(f"Gas estimation (transfer): {network_key}, from={masked_from}, to={masked_to}, amount={amount:.6f} USDT, gas={estimated_gas}")
        return estimated_gas
    except Exception as e:
        logger.error(f"Error estimating gas for transfer: {e}")
        raise RuntimeError(f"Failed to estimate gas for transfer: {e}")


def approve_usdt(
    w3: Web3,
    network_key: str,
    private_key: str,
    spender_address: str,
    amount: float,
    dry_run: bool = False
) -> Optional[str]:
    """
    Approve USDT spending (exact amount only).
    
    Args:
        w3: Web3 instance
        network_key: Network key
        private_key: Private key (hex with 0x)
        spender_address: Address to approve
        amount: Exact amount to approve
        dry_run: If True, don't broadcast transaction
    
    Returns:
        Transaction hash (None if dry_run)
    
    Raises:
        RuntimeError: If approval fails
    """
    try:
        account = Account.from_key(private_key)
        from_address = account.address
        
        contract = get_usdt_contract(w3, network_key)
        decimals = contract.functions.decimals().call()
        amount_wei = int(amount * (10 ** decimals))
        
        # Build transaction
        tx = contract.functions.approve(
            Web3.to_checksum_address(spender_address),
            amount_wei
        ).build_transaction({
            "from": from_address,
            "nonce": w3.eth.get_transaction_count(from_address),
            "gas": estimate_gas_for_approve(w3, network_key, from_address, spender_address, amount),
            "gasPrice": w3.eth.gas_price,
            "chainId": get_network_config(network_key)["chain_id"],
        })
        
        masked_from = f"{from_address[:6]}...{from_address[-4:]}" if len(from_address) > 10 else from_address
        masked_spender = f"{spender_address[:6]}...{spender_address[-4:]}" if len(spender_address) > 10 else spender_address
        gas_price = w3.from_wei(tx["gasPrice"], "gwei")
        gas_cost = w3.from_wei(tx["gas"] * tx["gasPrice"], "ether")
        
        if dry_run:
            logger.info(f"[DRY RUN] Approve transaction prepared:")
            logger.info(f"  Network: {network_key}")
            logger.info(f"  From: {masked_from}")
            logger.info(f"  Spender: {masked_spender}")
            logger.info(f"  Amount: {amount:.6f} USDT")
            logger.info(f"  Gas: {tx['gas']}")
            logger.info(f"  Gas Price: {gas_price:.2f} Gwei")
            logger.info(f"  Estimated Cost: {gas_cost:.6f} {get_network_config(network_key)['native_token']}")
            logger.info(f"  [DRY RUN] Transaction NOT sent - would approve {amount:.6f} USDT")
            return None
        
        # Sign and send
        logger.info(f"Signing approve transaction: {masked_from} -> {masked_spender}, amount={amount:.6f} USDT")
        signed_tx = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        tx_hash_hex = tx_hash.hex()
        
        logger.info(f"Approve transaction sent: {tx_hash_hex}, gas={tx['gas']}, gasPrice={gas_price:.2f} Gwei")
        return tx_hash_hex
        
    except Exception as e:
        logger.error(f"Error approving USDT: {e}")
        raise RuntimeError(f"Failed to approve USDT: {e}")


def transfer_usdt(
    w3: Web3,
    network_key: str,
    private_key: str,
    to_address: str,
    amount: float,
    dry_run: bool = False
) -> Optional[str]:
    """
    Transfer USDT to address.
    
    Args:
        w3: Web3 instance
        network_key: Network key
        private_key: Private key (hex with 0x)
        to_address: Recipient address
        amount: Amount to transfer
        dry_run: If True, don't broadcast transaction
    
    Returns:
        Transaction hash (None if dry_run)
    
    Raises:
        RuntimeError: If transfer fails
    """
    try:
        account = Account.from_key(private_key)
        from_address = account.address
        
        contract = get_usdt_contract(w3, network_key)
        decimals = contract.functions.decimals().call()
        amount_wei = int(amount * (10 ** decimals))
        
        # Build transaction
        tx = contract.functions.transfer(
            Web3.to_checksum_address(to_address),
            amount_wei
        ).build_transaction({
            "from": from_address,
            "nonce": w3.eth.get_transaction_count(from_address),
            "gas": estimate_gas_for_transfer(w3, network_key, from_address, to_address, amount),
            "gasPrice": w3.eth.gas_price,
            "chainId": get_network_config(network_key)["chain_id"],
        })
        
        masked_from = f"{from_address[:6]}...{from_address[-4:]}" if len(from_address) > 10 else from_address
        masked_to = f"{to_address[:6]}...{to_address[-4:]}" if len(to_address) > 10 else to_address
        gas_price = w3.from_wei(tx["gasPrice"], "gwei")
        gas_cost = w3.from_wei(tx["gas"] * tx["gasPrice"], "ether")
        
        if dry_run:
            logger.info(f"[DRY RUN] Transfer transaction prepared:")
            logger.info(f"  Network: {network_key}")
            logger.info(f"  From: {masked_from}")
            logger.info(f"  To: {masked_to}")
            logger.info(f"  Amount: {amount:.6f} USDT")
            logger.info(f"  Gas: {tx['gas']}")
            logger.info(f"  Gas Price: {gas_price:.2f} Gwei")
            logger.info(f"  Estimated Cost: {gas_cost:.6f} {get_network_config(network_key)['native_token']}")
            logger.info(f"  [DRY RUN] Transaction NOT sent - would transfer {amount:.6f} USDT")
            return None
        
        # Sign and send
        logger.info(f"Signing transfer transaction: {masked_from} -> {masked_to}, amount={amount:.6f} USDT")
        signed_tx = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        tx_hash_hex = tx_hash.hex()
        
        logger.info(f"Transfer transaction sent: {tx_hash_hex}, gas={tx['gas']}, gasPrice={gas_price:.2f} Gwei")
        return tx_hash_hex
        
    except Exception as e:
        logger.error(f"Error transferring USDT: {e}")
        raise RuntimeError(f"Failed to transfer USDT: {e}")


def check_allowance(w3: Web3, network_key: str, owner_address: str, spender_address: str) -> float:
    """
    Check current USDT allowance.
    
    Args:
        w3: Web3 instance
        network_key: Network key
        owner_address: Owner address
        spender_address: Spender address
    
    Returns:
        Current allowance (float)
    """
    try:
        contract = get_usdt_contract(w3, network_key)
        decimals = contract.functions.decimals().call()
        allowance_wei = contract.functions.allowance(
            Web3.to_checksum_address(owner_address),
            Web3.to_checksum_address(spender_address)
        ).call()
        allowance = allowance_wei / (10 ** decimals)
        return allowance
    except Exception as e:
        logger.error(f"Error checking allowance: {e}")
        raise RuntimeError(f"Failed to check allowance: {e}")
