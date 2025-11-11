pragma solidity ^0.8.20;

interface IDataProvenance {
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
    
    function logIngestion(
        string memory dataId,
        string memory dataHash,
        string memory source,
        string memory metadata
    ) external;
    
    function logTransformation(
        string memory dataId,
        string memory dataHash,
        string memory transformationType,
        string memory sourceDataId,
        string memory metadata
    ) external;
    
    function logStorage(
        string memory dataId,
        string memory dataHash,
        string memory storageLocation,
        string memory metadata
    ) external;
    
    function getFullLineage(string memory dataId) 
        external 
        view 
        returns (ProvenanceRecord[] memory);
    
    function verifyDataIntegrity(string memory dataId, string memory dataHash)
        external
        view
        returns (bool);
}