const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("DataProvenance", function () {
  let dataProvenance;
  let owner;
  let processor;
  let unauthorized;
  
  beforeEach(async function () {
    [owner, processor, unauthorized] = await ethers.getSigners();
    
    const DataProvenance = await ethers.getContractFactory("DataProvenance");
    dataProvenance = await DataProvenance.deploy();
    await dataProvenance.waitForDeployment();

    await dataProvenance.authorizeProcessor(processor.address);
  });
  
  describe("Deployment", function () {
    it("Should set the right owner", async function () {
      expect(await dataProvenance.owner()).to.equal(owner.address);
    });
    
    it("Should authorize deployer", async function () {
      expect(await dataProvenance.authorizedProcessors(owner.address)).to.be.true;
    });
  });
  
  describe("Ingestion Logging", function () {
    it("Should log ingestion event", async function () {
      const dataId = "data-001";
      const dataHash = "a".repeat(64);
      const source = "test-source";
      const metadata = '{"key":"value"}';
      
      await expect(
        dataProvenance.connect(processor).logIngestion(
          dataId, dataHash, source, metadata
        )
      ).to.emit(dataProvenance, "ProvenanceLogged");
      
      const recordCount = await dataProvenance.getRecordCount(dataId);
      expect(recordCount).to.equal(1);
    });
    
    it("Should reject unauthorized ingestion", async function () {
      const dataId = "data-001";
      const dataHash = "a".repeat(64);
      
      await expect(
        dataProvenance.connect(unauthorized).logIngestion(
          dataId, dataHash, "source", "{}"
        )
      ).to.be.revertedWith("Not authorized");
    });
    
    it("Should reject invalid hash length", async function () {
      await expect(
        dataProvenance.connect(processor).logIngestion(
          "data-001", "invalid", "source", "{}"
        )
      ).to.be.revertedWith("Invalid hash length");
    });
  });
  
  describe("Transformation Logging", function () {
    it("Should log transformation event", async function () {
      const dataId = "data-002";
      const dataHash = "b".repeat(64);
      const transformationType = "CLEAN";
      const sourceDataId = "data-001";
      const metadata = '{"transformation":"cleaning"}';
      
      await expect(
        dataProvenance.connect(processor).logTransformation(
          dataId, dataHash, transformationType, sourceDataId, metadata
        )
      ).to.emit(dataProvenance, "ProvenanceLogged");
    });
  });
  
  describe("Storage Logging", function () {
    it("Should log storage event", async function () {
      const dataId = "data-003";
      const dataHash = "c".repeat(64);
      const storageLocation = "s3://bucket/key";
      const metadata = '{}';
      
      await expect(
        dataProvenance.connect(processor).logStorage(
          dataId, dataHash, storageLocation, metadata
        )
      ).to.emit(dataProvenance, "ProvenanceLogged");
    });
  });
  
  describe("Lineage Retrieval", function () {
    it("Should retrieve full lineage", async function () {
      const dataId = "data-004";
      const hash1 = "d".repeat(64);
      const hash2 = "e".repeat(64);
      
      await dataProvenance.connect(processor).logIngestion(
        dataId, hash1, "source", "{}"
      );
      await dataProvenance.connect(processor).logTransformation(
        dataId, hash2, "TRANSFORM", dataId, "{}"
      );
      
      const lineage = await dataProvenance.getFullLineage(dataId);
      expect(lineage.length).to.equal(2);
      expect(lineage[0].dataHash).to.equal(hash1);
      expect(lineage[1].dataHash).to.equal(hash2);
    });
    
    it("Should verify data integrity", async function () {
      const dataId = "data-005";
      const dataHash = "f".repeat(64);
      
      await dataProvenance.connect(processor).logIngestion(
        dataId, dataHash, "source", "{}"
      );
      
      const isValid = await dataProvenance.verifyDataIntegrity(dataId, dataHash);
      expect(isValid).to.be.true;
      
      const isInvalid = await dataProvenance.verifyDataIntegrity(dataId, "invalid");
      expect(isInvalid).to.be.false;
    });
  });
  
  describe("Batch Operations", function () {
    it("Should log batch records", async function () {
      const dataIds = ["batch-001", "batch-002", "batch-003"];
      const hashes = ["1".repeat(64), "2".repeat(64), "3".repeat(64)];
      const types = ["INGESTION", "INGESTION", "INGESTION"];
      const sources = ["", "", ""];
      const metadata = ["{}", "{}", "{}"];
      
      await dataProvenance.connect(processor).logBatch(
        dataIds, hashes, types, sources, metadata
      );
      
      const count1 = await dataProvenance.getRecordCount(dataIds[0]);
      const count2 = await dataProvenance.getRecordCount(dataIds[1]);
      const count3 = await dataProvenance.getRecordCount(dataIds[2]);
      
      expect(count1).to.equal(1);
      expect(count2).to.equal(1);
      expect(count3).to.equal(1);
    });
  });
  
  describe("Access Control", function () {
    it("Should authorize new processor", async function () {
      const newProcessor = unauthorized.address;
      
      await expect(
        dataProvenance.authorizeProcessor(newProcessor)
      ).to.emit(dataProvenance, "ProcessorAuthorized");
      
      expect(await dataProvenance.authorizedProcessors(newProcessor)).to.be.true;
    });
    
    it("Should revoke processor", async function () {
      await expect(
        dataProvenance.revokeProcessor(processor.address)
      ).to.emit(dataProvenance, "ProcessorRevoked");
      
      expect(await dataProvenance.authorizedProcessors(processor.address)).to.be.false;
    });
  });
  
  describe("Contract Statistics", function () {
    it("Should track statistics correctly", async function () {
      const dataId = "stat-001";
      const dataHash = "9".repeat(64);
      
      await dataProvenance.connect(processor).logIngestion(
        dataId, dataHash, "source", "{}"
      );
      
      const stats = await dataProvenance.getContractStats();
      expect(stats._totalRecords).to.be.greaterThan(0);
      expect(stats._totalDataItems).to.be.greaterThan(0);
    });
  });
});