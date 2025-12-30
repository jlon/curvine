#!/bin/bash
# Batch optimize logging levels for high-frequency operations
# 
# Strategy:
# - High-frequency operations (COMPOUND, READ, WRITE, GETATTR, etc.) -> DEBUG
# - State changes (OPEN, CLOSE, CREATE, REMOVE) -> Keep INFO
# - Errors -> Keep ERROR

set -e

echo "=========================================="
echo "NFSv4 Gateway Logging Optimization"
echo "=========================================="
echo ""

# Backup original files
echo "Creating backups..."
cp curvine-nfs/src/nfs4/handlers.rs curvine-nfs/src/nfs4/handlers.rs.bak

# Optimize handlers.rs - high-frequency operations
echo "Optimizing handlers.rs..."

# PUTROOTFH, GETFH, PUTFH - very high frequency
sed -i 's/info!(\s*$/debug!(/' curvine-nfs/src/nfs4/handlers.rs
sed -i 's/"PUTROOTFH:/"[DEBUG] PUTROOTFH:/' curvine-nfs/src/nfs4/handlers.rs
sed -i 's/"GETFH:/"[DEBUG] GETFH:/' curvine-nfs/src/nfs4/handlers.rs

# SETCLIENTID operations - keep some as INFO, downgrade details to DEBUG
sed -i 's/"NFSv4.0 SETCLIENTID: response/"[DEBUG] NFSv4.0 SETCLIENTID: response/' curvine-nfs/src/nfs4/handlers.rs

echo "✅ handlers.rs optimized"

# Optimize state/client.rs - downgrade frequent client operations
if [ -f curvine-nfs/src/nfs4/state/client.rs ]; then
    echo "Optimizing state/client.rs..."
    cp curvine-nfs/src/nfs4/state/client.rs curvine-nfs/src/nfs4/state/client.rs.bak
    
    # Downgrade "Found confirmed client" to DEBUG (happens on every operation)
    sed -i 's/tracing::info!("Found confirmed client/tracing::debug!("Found confirmed client/' curvine-nfs/src/nfs4/state/client.rs
    
    echo "✅ state/client.rs optimized"
fi

# Optimize state/persistence.rs - downgrade periodic saves
if [ -f curvine-nfs/src/nfs4/state/persistence.rs ]; then
    echo "Optimizing state/persistence.rs..."
    cp curvine-nfs/src/nfs4/state/persistence.rs curvine-nfs/src/nfs4/state/persistence.rs.bak
    
    # Downgrade "State snapshot saved" to DEBUG (happens every 30 seconds)
    sed -i 's/info!("State snapshot saved/debug!("State snapshot saved/' curvine-nfs/src/nfs4/state/persistence.rs
    
    echo "✅ state/persistence.rs optimized"
fi

# Optimize state/grace.rs - downgrade grace period messages
if [ -f curvine-nfs/src/nfs4/state/grace.rs ]; then
    echo "Optimizing state/grace.rs..."
    cp curvine-nfs/src/nfs4/state/grace.rs curvine-nfs/src/nfs4/state/grace.rs.bak
    
    # Downgrade grace period reaper to DEBUG
    sed -i 's/info!("Grace period reaper/debug!("Grace period reaper/' curvine-nfs/src/nfs4/state/grace.rs
    
    echo "✅ state/grace.rs optimized"
fi

echo ""
echo "=========================================="
echo "Optimization Complete"
echo "=========================================="
echo ""
echo "Modified files:"
echo "  - curvine-nfs/src/nfs4/handlers.rs"
echo "  - curvine-nfs/src/nfs4/state/client.rs"
echo "  - curvine-nfs/src/nfs4/state/persistence.rs"
echo "  - curvine-nfs/src/nfs4/state/grace.rs"
echo ""
echo "Backups created with .bak extension"
echo ""
echo "Next steps:"
echo "  1. cargo build --release -p curvine-nfs"
echo "  2. Deploy and restart service"
echo "  3. Monitor log output"
