const hre = require("hardhat");
const fs = require("fs");
const path = require("path");

async function main() {
  console.log("=".repeat(70));
  console.log("Deploying DataProvenance Contract");
  console.log("=".repeat(70));
  
  const [deployer] = await hre.ethers.getSigners();
  const network = hre.network.name;
  
  console.log("\\nDeployment Info:");
  console.log("- Network:", network);
  console.log("- Deployer:", deployer.address);
  console.log("- Balance:", hre.ethers.formatEther(
    await hre.ethers.provider.getBalance(deployer.address)
  ), "MATIC");
  
  console.log("\\nDeploying contract...");
  const DataProvenance = await hre.ethers.getContractFactory("DataProvenance");
  const contract = await DataProvenance.deploy();
  
  await contract.waitForDeployment();
  const contractAddress = await contract.getAddress();
  
  console.log("✓ Contract deployed to:", contractAddress);
  
  if (network !== "hardhat" && network !== "localhost") {
    console.log("\\nWaiting for block confirmations...");
    await contract.deploymentTransaction().wait(6);
    console.log("✓ Confirmed");
  }

  const deploymentInfo = {
    network: network,
    contractAddress: contractAddress,
    deployer: deployer.address,
    deploymentTime: new Date().toISOString(),
    blockNumber: await hre.ethers.provider.getBlockNumber(),
    transactionHash: contract.deploymentTransaction().hash
  };
  
  const deploymentPath = path.join(__dirname, "../../deployment/deployed_contracts.json");
  let deployments = {};
  
  if (fs.existsSync(deploymentPath)) {
    deployments = JSON.parse(fs.readFileSync(deploymentPath, "utf8"));
  }
  
  deployments[network] = deploymentInfo;
  
  fs.mkdirSync(path.dirname(deploymentPath), { recursive: true });
  fs.writeFileSync(deploymentPath, JSON.stringify(deployments, null, 2));
  
  console.log("\\n✓ Deployment info saved to:", deploymentPath);

  const artifactPath = path.join(__dirname, "../artifacts/contracts/DataProvenance.sol/DataProvenance.json");
  const artifact = JSON.parse(fs.readFileSync(artifactPath, "utf8"));
  const abiPath = path.join(__dirname, "../../deployment/DataProvenance.abi.json");
  
  fs.writeFileSync(abiPath, JSON.stringify(artifact.abi, null, 2));
  console.log("✓ ABI saved to:", abiPath);

  if (network === "mumbai") {
    console.log("\\nTo verify on Polygonscan:");
    console.log(`npx hardhat verify --network mumbai ${contractAddress}`);
  }
  
  console.log("\\n" + "=".repeat(70));
  console.log("Deployment Complete!");
  console.log("=".repeat(70));
  console.log("\\nContract Address:", contractAddress);
  console.log("Network:", network);
  console.log("\\nNext steps:");
  console.log("1. Update .env with CONTRACT_ADDRESS");
  console.log("2. Run verification script if on testnet");
  console.log("3. Test the contract with Python client");
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });