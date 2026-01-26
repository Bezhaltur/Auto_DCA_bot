# Auto USDT Sending Feature

## Overview

The bot now supports automatic USDT sending from EVM wallets to execute FixedFloat orders. This feature eliminates the need for manual token transfers.

## Supported Networks

- **Arbitrum** (USDT-ARB)
- **BSC** (USDT-BSC)  
- **Polygon** (USDT-MATIC)

## Security Features

### Wallet Storage
- User provides **standard Ethereum JSON keystore files**
- Keystore files stored in `keystores/` directory with restrictive permissions (600)
- Private keys are **never** stored in plaintext, env files, logs, or database
- Decryption happens only in memory during transaction execution using `eth_account.Account.decrypt`
- **No custom encryption** - uses standard Ethereum keystore format only

### Password Management
- Passwords are stored **ONLY in memory** (not in database, not on disk)
- Passwords are cleared when bot restarts
- User must set password via `/setpassword` command for automatic sending
- Password is required for each keystore decryption

## Setup

### 1. Create Ethereum Keystore File

Create a standard Ethereum JSON keystore file using any compatible tool:
- MetaMask (export account)
- MyEtherWallet
- `eth-keyfile` library
- Any tool that generates standard Ethereum keystore format

### 2. Configure Wallet

Use the `/setwallet` command with your keystore file:
```
/setwallet USDT-ARB /path/to/keystore.json mypassword
```

Or paste keystore JSON directly:
```
/setwallet USDT-ARB {"crypto":{...},"address":"0x..."} mypassword
```

Parameters:
- **Network**: USDT-ARB, USDT-BSC, or USDT-MATIC
- **Keystore**: Path to keystore file OR JSON string
- **Password**: Password for keystore decryption

### 3. Set Password for Auto-Send

After configuring wallet, set password in memory for automatic sending:
```
/setpassword USDT-ARB mypassword
```

**Note**: Password is stored only in memory and will be cleared on bot restart.

### 3. Check Wallet Status

```
/walletstatus
```

Shows:
- Configured networks
- Wallet addresses
- USDT balances
- Native token balances (ETH/BNB/MATIC)

## Auto Execution Flow

1. **DCA Order Created**: Bot creates FixedFloat order (USDT → BTC)
2. **Order Data Received**: 
   - Deposit address
   - Exact USDT amount
   - BTC destination address
3. **Pre-flight Checks** (all must pass):
   - ✅ Deposit address format validation
   - ✅ BTC address matches order configuration
   - ✅ USDT balance >= required amount
   - ✅ Native token balance >= estimated gas
4. **ERC20 Approval** (if needed):
   - Approves **exact amount only** (no unlimited approvals)
   - Separate approval for each network
   - Waits for approval confirmation
5. **USDT Transfer**:
   - Sends exact amount to FixedFloat deposit address
   - Waits for transaction confirmation
6. **Notifications**:
   - Success: Transaction hashes and explorer links
   - Failure: Detailed error message with manual instructions

## Safety Features

### Address Validation
- Only sends to FixedFloat deposit addresses from created orders
- Validates deposit address format before sending
- Verifies BTC address matches order configuration

### Balance Checks
- USDT balance check before approval/transfer
- Native token balance check for gas estimation
- Aborts if any check fails

### Gas Estimation
- Estimates gas for both approve and transfer
- Uses 1.2x multiplier for safety margin
- Checks native balance with 1.5x multiplier

### Error Handling
- All errors are logged
- User notifications for all failure cases:
  - Insufficient USDT balance
  - Insufficient native token for gas
  - Approval transaction failure
  - Transfer transaction failure
  - Network/RPC errors

## Commands

### `/setwallet СЕТЬ ПУТЬ_К_ФАЙЛУ ПАРОЛЬ` or `/setwallet СЕТЬ JSON ПАРОЛЬ`
Configure wallet using standard Ethereum JSON keystore file.

### `/setpassword СЕТЬ ПАРОЛЬ`
Set password in memory for automatic sending (required for auto-send).

### `/clearpassword [СЕТЬ]`
Clear password from memory. If network not specified, clears all passwords.

### `/walletstatus`
Show wallet status and balances.

### `/deletewallet СЕТЬ`
Delete wallet configuration for a network.

## Dry-Run Mode

Set `DRY_RUN=true` in `.env` to test without broadcasting transactions:
- All checks are performed
- Transactions are built but not sent
- Useful for testing and validation

## Order Monitoring

The bot monitors completed orders and sends notifications:
- Checks order status periodically
- Sends Blockchair link for BTC transactions
- Notifies when order is processed by FixedFloat

## Technical Details

### ERC20 Operations
- Uses web3.py for all blockchain interactions
- RPC calls only (no explorer APIs)
- Supports standard ERC20 functions:
  - `balanceOf(address)`
  - `approve(spender, amount)`
  - `transfer(to, amount)`
  - `allowance(owner, spender)`

### Network Configuration
All network configs centralized in `networks.py`:
- RPC endpoints
- Chain IDs
- USDT contract addresses
- Native token symbols
- Explorer base URLs

### Database Schema

**wallets** table:
- `user_id`: Telegram user ID
- `network_key`: Network identifier
- `wallet_address`: Wallet address (checksummed)
- `encrypted_password`: Encrypted wallet password

**sent_transactions** table:
- Tracks all sent transactions
- Links to orders and plans
- Stores transaction hashes

**completed_orders** table:
- Tracks completed FixedFloat orders
- Stores BTC transaction IDs when available
- Prevents duplicate notifications

## Error Messages

### Insufficient USDT Balance
```
❌ Недостаточно USDT баланса
Требуется: X.XXXXXX USDT
Доступно: Y.YYYYYY USDT
Недостача: Z.ZZZZZZ USDT
```

### Insufficient Native Token for Gas
```
❌ Недостаточно {TOKEN} баланса для газа
Требуется: X.XXXXXX {TOKEN}
Доступно: Y.YYYYYY {TOKEN}
Недостача: Z.ZZZZZZ {TOKEN}
```

### Wallet Not Configured
```
❌ Кошелёк не настроен для {NETWORK}
Используй /setwallet для настройки
```

## Best Practices

1. **Keystore Security**:
   - Use standard Ethereum JSON keystore format
   - Keep keystore files secure (restrictive permissions)
   - Never share keystore files or passwords

2. **Password Management**:
   - Passwords stored only in memory
   - Set password after configuring wallet: `/setpassword`
   - Password cleared on bot restart (must be set again)
   - Use `/clearpassword` to remove from memory

3. **Wallet Security**:
   - Use dedicated wallets for DCA
   - Keep sufficient native token for gas
   - Never share private keys or keystore files

4. **Testing**:
   - Use dry-run mode first (`DRY_RUN=true` in `.env`)
   - Test with small amounts
   - Verify on testnets if possible

## Troubleshooting

### "Wallet not configured"
- Run `/setwallet` command
- Ensure network key matches (USDT-ARB, USDT-BSC, USDT-MATIC)

### "Incorrect wallet password"
- Password is for keystore decryption (standard Ethereum format)
- Verify password is correct for the keystore file
- Use `/setpassword` to update password in memory

### "Failed to connect to RPC"
- Check network connectivity
- Verify RPC endpoints in `networks.py`
- Some RPCs may have rate limits

### "Transaction failed"
- Check gas price (may be too low)
- Verify contract addresses are correct
- Check if USDT contract supports the operation

## Files

- `networks.py`: Network configuration
- `wallet.py`: Keystore encryption/decryption
- `erc20.py`: ERC20 operations (balance, approve, transfer)
- `auto_send.py`: Auto-send logic and checks
- `bot.py`: Integration and Telegram commands

## Future Enhancements

- Support for additional networks
- Gas price optimization
- Transaction retry logic
- Webhook support for order status
- Multi-signature wallet support
