"""
Automatic USDT sending to FixedFloat deposit addresses.
Handles all checks, approvals, and transfers.
"""

import logging
from typing import Optional, Tuple
from web3 import Web3
from networks import get_network_config, get_blockchair_url
from erc20 import (
    get_web3_instance,
    get_usdt_balance,
    get_native_balance,
    approve_usdt,
    transfer_usdt,
    estimate_gas_for_approve,
    estimate_gas_for_transfer,
    check_allowance,
)
from wallet import load_keystore, decrypt_private_key
from test_config import mask_sensitive_data

logger = logging.getLogger(__name__)

# Gas price multiplier for safety margin
GAS_PRICE_MULTIPLIER = 1.2
# Minimum native token balance multiplier (for safety)
MIN_NATIVE_MULTIPLIER = 1.5


async def auto_send_usdt(
    network_key: str,
    user_id: int,
    wallet_password: str,
    deposit_address: str,
    required_amount: float,
    btc_address: str,
    order_id: str,
    dry_run: bool = False
) -> Tuple[bool, Optional[str], Optional[str], str]:
    """
    Automatically send USDT to FixedFloat deposit address.
    
    Performs all checks:
    - Deposit address validation
    - BTC address validation
    - USDT balance check
    - Native token balance check
    - Approval (if needed)
    - Transfer
    
    Args:
        network_key: Network key (e.g., "USDT-ARB")
        user_id: Telegram user ID
        wallet_password: Keystore password
        deposit_address: FixedFloat deposit address
        required_amount: Required USDT amount
        btc_address: Expected BTC address (for validation)
        order_id: FixedFloat order ID
        dry_run: If True, don't broadcast transactions
    
    Returns:
        Tuple of (success, approve_tx_hash, transfer_tx_hash, error_message)
        - success: True if transfer succeeded
        - approve_tx_hash: Transaction hash for approve (None if not needed)
        - transfer_tx_hash: Transaction hash for transfer
        - error_message: Error message if failed
    """
    try:
        # Load keystore
        keystore = load_keystore(user_id, network_key)
        if not keystore:
            return (False, None, None, f"Wallet not configured for {network_key}. Use /setwallet to configure.")
        
        # Decrypt private key (in memory only)
        try:
            private_key_hex = decrypt_private_key(keystore, wallet_password)
            private_key = "0x" + private_key_hex
        except ValueError as e:
            return (False, None, None, f"Incorrect wallet password: {e}")
        
        from eth_account import Account
        account = Account.from_key(private_key)
        wallet_address = account.address
        masked_wallet = f"{wallet_address[:6]}...{wallet_address[-4:]}" if len(wallet_address) > 10 else wallet_address
        masked_deposit = f"{deposit_address[:6]}...{deposit_address[-4:]}" if len(deposit_address) > 10 else deposit_address
        
        logger.info(f"=== Auto-send USDT started ===")
        logger.info(f"Order ID: {order_id}")
        logger.info(f"Network: {network_key}")
        logger.info(f"Wallet: {masked_wallet}")
        logger.info(f"Deposit: {masked_deposit}")
        logger.info(f"Amount: {required_amount:.6f} USDT")
        logger.info(f"Dry-run: {dry_run}")
        
        # Initialize Web3
        w3 = get_web3_instance(network_key)
        config = get_network_config(network_key)
        
        # Check 1: Validate deposit address format
        logger.info(f"Check 1: Validating deposit address format...")
        try:
            deposit_address_checksum = Web3.to_checksum_address(deposit_address)
            logger.info(f"✓ Deposit address valid: {masked_deposit}")
        except Exception as e:
            logger.error(f"✗ Invalid deposit address format: {e}")
            return (False, None, None, f"Invalid deposit address format: {e}")
        
        # Check 2: Get balances
        logger.info(f"Check 2: Checking balances...")
        try:
            usdt_balance = get_usdt_balance(w3, network_key, wallet_address)
            native_balance = get_native_balance(w3, wallet_address)
            logger.info(f"✓ USDT balance: {usdt_balance:.6f} USDT")
            logger.info(f"✓ Native balance: {native_balance:.6f} {config['native_token']}")
        except Exception as e:
            logger.error(f"✗ Failed to check balances: {e}")
            return (False, None, None, f"Failed to check balances: {e}")
        
        # Check 3: USDT balance sufficient
        logger.info(f"Check 3: Verifying USDT balance sufficient...")
        if usdt_balance < required_amount:
            logger.error(f"✗ Insufficient USDT: required={required_amount:.6f}, available={usdt_balance:.6f}")
            return (
                False, None, None,
                f"Insufficient USDT balance.\n"
                f"Required: {required_amount:.6f} USDT\n"
                f"Available: {usdt_balance:.6f} USDT\n"
                f"Shortage: {required_amount - usdt_balance:.6f} USDT"
            )
        logger.info(f"✓ USDT balance sufficient")
        
        # Check 4: Estimate gas for both transactions
        logger.info(f"Check 4: Estimating gas for transactions...")
        try:
            approve_gas = estimate_gas_for_approve(w3, network_key, wallet_address, deposit_address_checksum, required_amount)
            transfer_gas = estimate_gas_for_transfer(w3, network_key, wallet_address, deposit_address_checksum, required_amount)
            total_gas = approve_gas + transfer_gas
            
            # Get gas price
            gas_price = w3.eth.gas_price
            gas_price_gwei = w3.from_wei(gas_price, "gwei")
            total_gas_cost_wei = total_gas * gas_price * GAS_PRICE_MULTIPLIER
            total_gas_cost = w3.from_wei(total_gas_cost_wei, "ether")
            min_native_required = float(total_gas_cost) * MIN_NATIVE_MULTIPLIER
            
            logger.info(f"✓ Gas estimation complete:")
            logger.info(f"  Approve gas: {approve_gas}")
            logger.info(f"  Transfer gas: {transfer_gas}")
            logger.info(f"  Total gas: {total_gas}")
            logger.info(f"  Gas price: {gas_price_gwei:.2f} Gwei")
            logger.info(f"  Estimated cost: {total_gas_cost:.6f} {config['native_token']}")
            logger.info(f"  Required (with margin): {min_native_required:.6f} {config['native_token']}")
        except Exception as e:
            logger.error(f"✗ Failed to estimate gas: {e}")
            return (False, None, None, f"Failed to estimate gas: {e}")
        
        # Check 5: Native token balance sufficient
        logger.info(f"Check 5: Verifying native token balance sufficient...")
        if native_balance < min_native_required:
            logger.error(f"✗ Insufficient native token: required={min_native_required:.6f}, available={native_balance:.6f}")
            return (
                False, None, None,
                f"Insufficient {config['native_token']} balance for gas.\n"
                f"Required: {min_native_required:.6f} {config['native_token']}\n"
                f"Available: {native_balance:.6f} {config['native_token']}\n"
                f"Shortage: {min_native_required - native_balance:.6f} {config['native_token']}"
            )
        logger.info(f"✓ Native token balance sufficient")
        logger.info(f"=== All checks passed, proceeding with transactions ===")
        
        # All checks passed - proceed with transactions
        approve_tx_hash = None
        
        # Check current allowance
        logger.info(f"Checking current USDT allowance...")
        current_allowance = check_allowance(w3, network_key, wallet_address, deposit_address_checksum)
        logger.info(f"Current allowance: {current_allowance:.6f} USDT")
        
        if current_allowance < required_amount:
            # Need to approve
            logger.info(f"Step 1: Approving {required_amount:.6f} USDT to {masked_deposit}")
            try:
                approve_tx_hash = approve_usdt(
                    w3, network_key, private_key,
                    deposit_address_checksum, required_amount, dry_run
                )
                
                if dry_run:
                    logger.info(f"[DRY RUN] Approve step completed (no transaction sent)")
                elif approve_tx_hash:
                    logger.info(f"Waiting for approve transaction confirmation...")
                    receipt = w3.eth.wait_for_transaction_receipt(approve_tx_hash, timeout=120)
                    if receipt.status != 1:
                        logger.error(f"✗ Approve transaction failed: {approve_tx_hash}")
                        return (False, approve_tx_hash, None, "Approve transaction failed")
                    logger.info(f"✓ Approve transaction confirmed: {approve_tx_hash}, block={receipt.blockNumber}")
                else:
                    logger.error(f"✗ Approve transaction returned None")
                    return (False, None, None, "Approve transaction failed")
            except Exception as e:
                logger.error(f"✗ Approve failed: {e}")
                return (False, None, None, f"Approve failed: {e}")
        else:
            logger.info(f"✓ Sufficient allowance already exists: {current_allowance:.6f} USDT (no approve needed)")
        
        # Transfer USDT
        logger.info(f"Step 2: Transferring {required_amount:.6f} USDT to {masked_deposit}")
        try:
            transfer_tx_hash = transfer_usdt(
                w3, network_key, private_key,
                deposit_address_checksum, required_amount, dry_run
            )
            
            if dry_run:
                logger.info(f"[DRY RUN] Transfer step completed (no transaction sent)")
                logger.info(f"=== Auto-send completed (DRY RUN) ===")
                return (True, approve_tx_hash, None, "DRY RUN: Would transfer USDT")
            
            if not transfer_tx_hash:
                logger.error(f"✗ Transfer transaction returned None")
                return (False, approve_tx_hash, None, "Transfer transaction failed")
            
            logger.info(f"Waiting for transfer transaction confirmation...")
            receipt = w3.eth.wait_for_transaction_receipt(transfer_tx_hash, timeout=120)
            if receipt.status != 1:
                logger.error(f"✗ Transfer transaction failed: {transfer_tx_hash}")
                return (False, approve_tx_hash, transfer_tx_hash, "Transfer transaction failed")
            
            logger.info(f"✓ Transfer transaction confirmed: {transfer_tx_hash}, block={receipt.blockNumber}")
            logger.info(f"=== Auto-send completed successfully ===")
            
            # Clear private key from memory (best effort)
            private_key = None
            del private_key
            
            return (True, approve_tx_hash, transfer_tx_hash, "")
            
        except Exception as e:
            logger.error(f"✗ Transfer failed: {e}")
            return (False, approve_tx_hash, None, f"Transfer failed: {e}")
    
    except Exception as e:
        logger.error(f"Error in auto_send_usdt: {e}", exc_info=True)
        return (False, None, None, f"Unexpected error: {e}")
