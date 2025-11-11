echo "===================================================================="
echo "Data Provenance Engine - Blockchain Setup"
echo "===================================================================="

if ! command -v node &> /dev/null; then
    echo "❌ Node.js not found. Please install Node.js first."
    exit 1
fi

echo "✓ Node.js found: $(node --version)"

echo ""
echo "📦 Installing Hardhat dependencies..."
cd hardhat
npm install

echo ""
echo "🔨 Compiling smart contracts..."
npx hardhat compile

if [ $? -eq 0 ]; then
    echo "✓ Contracts compiled successfully"
else
    echo "❌ Contract compilation failed"
    exit 1
fi

echo ""
echo "🧪 Running contract tests..."
npx hardhat test

if [ $? -eq 0 ]; then
    echo "✓ All tests passed"
else
    echo "⚠️  Some tests failed"
fi

echo ""
echo "📦 Installing Python dependencies..."
cd ..
pip install -r requirements.txt

echo ""
echo "===================================================================="
echo "Setup Complete!"
echo "===================================================================="
echo ""
echo "Next steps:"
echo "1. Create .env file (copy from .env.example)"
echo "2. Get testnet MATIC from faucet: https://faucet.polygon.technology/"
echo "3. Deploy contract: cd hardhat && npm run deploy:mumbai"
echo "4. Update .env with CONTRACT_ADDRESS"
echo "5. Run tests: python -m pytest python/tests/"
echo ""
