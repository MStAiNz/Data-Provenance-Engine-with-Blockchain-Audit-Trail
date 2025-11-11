pragma solidity ^0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";

contract DataProvenance is Ownable, ReentrancyGuard, Pausable {
        
    struct ProvenanceRecord {
        string dataId;              
        string dataHash;            
        string transformationType;  
        string sourceDataId;        
        address processor;          
        uint256 timestamp;          
        string metadata;            
        bool exists;                
    }
    
    struct DataLineage {
        string dataId;
        uint256 recordCount;
        uint256 firstRecordTimestamp;
        uint256 lastRecordTimestamp;
        bool isActive;
    }
    
    mapping(string => ProvenanceRecord[]) private dataLineage;
    
    mapping(string => DataLineage) private lineageMetadata;
    
    mapping(string => string) private hashToDataId;
    
    mapping(address => bool) public authorizedProcessors;
    
    uint256 public totalRecords;
    uint256 public totalDataItems;
    
    event ProvenanceLogged(
        string indexed dataId,
        string dataHash,
        string transformationType,
        address indexed processor,
        uint256 timestamp
    );
    
    event ProcessorAuthorized(address indexed processor);
    event ProcessorRevoked(address indexed processor);
    event DataLineageCompleted(string indexed dataId, uint256 recordCount);
    
    modifier onlyAuthorized() {
        require(
            authorizedProcessors[msg.sender] || msg.sender == owner(),
            "Not authorized"
        );
        _;
    }
    
    modifier validDataId(string memory dataId) {
        require(bytes(dataId).length > 0, "Invalid dataId");
        _;
    }
    
    modifier validDataHash(string memory dataHash) {
        require(bytes(dataHash).length == 64, "Invalid hash length");
        _;
    }

    
    constructor() {
        authorizedProcessors[msg.sender] = true;
    }

    function logIngestion(
        string memory dataId,
        string memory dataHash,
        string memory source,
        string memory metadata
    ) 
        external 
        onlyAuthorized 
        whenNotPaused
        nonReentrant
        validDataId(dataId)
        validDataHash(dataHash)
    {
        _logProvenance(
            dataId,
            dataHash,
            "INGESTION",
            "",
            metadata
        );
    
        if (!lineageMetadata[dataId].isActive) {
            lineageMetadata[dataId] = DataLineage({
                dataId: dataId,
                recordCount: 1,
                firstRecordTimestamp: block.timestamp,
                lastRecordTimestamp: block.timestamp,
                isActive: true
            });
            totalDataItems++;
        }
    }

    function logTransformation(
        string memory dataId,
        string memory dataHash,
        string memory transformationType,
        string memory sourceDataId,
        string memory metadata
    )
        external
        onlyAuthorized
        whenNotPaused
        nonReentrant
        validDataId(dataId)
        validDataHash(dataHash)
    {
        require(
            bytes(transformationType).length > 0,
            "Invalid transformation type"
        );
        
        _logProvenance(
            dataId,
            dataHash,
            transformationType,
            sourceDataId,
            metadata
        );
        
        _updateLineageMetadata(dataId);
    }

    function logStorage(
        string memory dataId,
        string memory dataHash,
        string memory storageLocation,
        string memory metadata
    )
        external
        onlyAuthorized
        whenNotPaused
        nonReentrant
        validDataId(dataId)
        validDataHash(dataHash)
    {
        string memory meta = string(
            abi.encodePacked(
                '{"storage_location":"', storageLocation, '",',
                '"metadata":', metadata, '}'
            )
        );
        
        _logProvenance(
            dataId,
            dataHash,
            "STORAGE",
            "",
            meta
        );
        
        _updateLineageMetadata(dataId);
    }
    
    function logBatch(
        string[] memory dataIds,
        string[] memory dataHashes,
        string[] memory transformationTypes,
        string[] memory sourceDataIds,
        string[] memory metadataArray
    )
        external
        onlyAuthorized
        whenNotPaused
        nonReentrant
    {
        require(
            dataIds.length == dataHashes.length &&
            dataIds.length == transformationTypes.length &&
            dataIds.length == sourceDataIds.length &&
            dataIds.length == metadataArray.length,
            "Array length mismatch"
        );
        
        require(dataIds.length <= 100, "Batch too large");
        
        for (uint256 i = 0; i < dataIds.length; i++) {
            _logProvenance(
                dataIds[i],
                dataHashes[i],
                transformationTypes[i],
                sourceDataIds[i],
                metadataArray[i]
            );
            _updateLineageMetadata(dataIds[i]);
        }
    }

    function completeLineage(string memory dataId) 
        external 
        onlyAuthorized 
        validDataId(dataId)
    {
        require(lineageMetadata[dataId].isActive, "Lineage not active");
        
        lineageMetadata[dataId].isActive = false;
        
        emit DataLineageCompleted(
            dataId, 
            lineageMetadata[dataId].recordCount
        );
    }
    
    function getProvenanceRecord(string memory dataId, uint256 index)
        external
        view
        returns (ProvenanceRecord memory)
    {
        require(index < dataLineage[dataId].length, "Index out of bounds");
        return dataLineage[dataId][index];
    }

    function getFullLineage(string memory dataId)
        external
        view
        returns (ProvenanceRecord[] memory)
    {
        return dataLineage[dataId];
    }
    
    function getLineageMetadata(string memory dataId)
        external
        view
        returns (DataLineage memory)
    {
        return lineageMetadata[dataId];
    }
    
    function getRecordCount(string memory dataId)
        external
        view
        returns (uint256)
    {
        return dataLineage[dataId].length;
    }

    function getDataIdByHash(string memory dataHash)
        external
        view
        returns (string memory)
    {
        return hashToDataId[dataHash];
    }

    function verifyDataIntegrity(string memory dataId, string memory dataHash)
        external
        view
        returns (bool)
    {
        ProvenanceRecord[] memory records = dataLineage[dataId];
        if (records.length == 0) return false;
        
        return keccak256(bytes(records[records.length - 1].dataHash)) 
            == keccak256(bytes(dataHash));
    }
    
    function authorizeProcessor(address processor) external onlyOwner {
        require(processor != address(0), "Invalid address");
        require(!authorizedProcessors[processor], "Already authorized");
        
        authorizedProcessors[processor] = true;
        emit ProcessorAuthorized(processor);
    }

    function revokeProcessor(address processor) external onlyOwner {
        require(authorizedProcessors[processor], "Not authorized");
        
        authorizedProcessors[processor] = false;
        emit ProcessorRevoked(processor);
    }
    
    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
    }
    
    function _logProvenance(
        string memory dataId,
        string memory dataHash,
        string memory transformationType,
        string memory sourceDataId,
        string memory metadata
    ) private {
        ProvenanceRecord memory record = ProvenanceRecord({
            dataId: dataId,
            dataHash: dataHash,
            transformationType: transformationType,
            sourceDataId: sourceDataId,
            processor: msg.sender,
            timestamp: block.timestamp,
            metadata: metadata,
            exists: true
        });
        
        dataLineage[dataId].push(record);
        hashToDataId[dataHash] = dataId;
        totalRecords++;
        
        emit ProvenanceLogged(
            dataId,
            dataHash,
            transformationType,
            msg.sender,
            block.timestamp
        );
    }
    
    function _updateLineageMetadata(string memory dataId) private {
        if (!lineageMetadata[dataId].isActive) {
            lineageMetadata[dataId] = DataLineage({
                dataId: dataId,
                recordCount: 1,
                firstRecordTimestamp: block.timestamp,
                lastRecordTimestamp: block.timestamp,
                isActive: true
            });
            totalDataItems++;
        } else {
            lineageMetadata[dataId].recordCount++;
            lineageMetadata[dataId].lastRecordTimestamp = block.timestamp;
        }
    }
    
    function getContractStats() 
        external 
        view 
        returns (
            uint256 _totalRecords,
            uint256 _totalDataItems
        ) 
    {
        return (totalRecords, totalDataItems);
    }
}