-- Contract Management: Raw + Split (Header/Batch/Wagon) + Structure
-- Ziel:
-- 1) Tagesimport des CM-Sheets
-- 2) Aufteilung nach Row Type (Header, Batch, Wagon)
-- 3) Delta-History je Datentyp
-- 4) Structure-Tabelle fuer Reihenfolge/Wiederaufbau in Excel

IF OBJECT_ID('landing.ContractManagement_raw', 'U') IS NULL
BEGIN
    CREATE TABLE landing.ContractManagement_raw (
        raw_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        import_timestamp_utc DATETIME2(0) NOT NULL,
        batch_id NVARCHAR(64) NOT NULL,
        source_file NVARCHAR(1024) NULL,
        source_sheet NVARCHAR(255) NULL,
        source_row_index INT NULL,
        business_key_hash CHAR(64) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        row_json NVARCHAR(MAX) NULL
    );
    CREATE INDEX IX_ContractManagement_raw_import_ts ON landing.ContractManagement_raw(import_timestamp_utc);
    CREATE INDEX IX_ContractManagement_raw_key_hash ON landing.ContractManagement_raw(business_key_hash);
END;
GO

IF OBJECT_ID('landing.ContractManagement_Structure', 'U') IS NULL
BEGIN
    CREATE TABLE landing.ContractManagement_Structure (
        structure_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        import_timestamp_utc DATETIME2(0) NOT NULL,
        batch_id NVARCHAR(64) NOT NULL,
        source_file NVARCHAR(1024) NULL,
        source_sheet NVARCHAR(255) NULL,
        source_row_index INT NULL,
        row_sequence INT NOT NULL,
        row_type NVARCHAR(30) NOT NULL,
        entity_table NVARCHAR(255) NOT NULL,
        business_key_hash CHAR(64) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        row_json NVARCHAR(MAX) NULL
    );

    CREATE INDEX IX_ContractManagement_Structure_import_ts
        ON landing.ContractManagement_Structure(import_timestamp_utc);
    CREATE INDEX IX_ContractManagement_Structure_batch_seq
        ON landing.ContractManagement_Structure(batch_id, row_sequence);
    CREATE INDEX IX_ContractManagement_Structure_hash
        ON landing.ContractManagement_Structure(business_key_hash, record_hash);
END;
GO

IF OBJECT_ID('landing.ContractManagement_Header', 'U') IS NULL
BEGIN
    CREATE TABLE landing.ContractManagement_Header (
        contract_number_m3 NVARCHAR(120) NULL,
        contract_number_customer NVARCHAR(120) NULL,
        rentalposition_m3 NVARCHAR(120) NULL,
        remarks NVARCHAR(2000) NULL,
        row_type NVARCHAR(30) NULL,
        wagon_number NVARCHAR(120) NULL,
        customer NVARCHAR(200) NULL,
        row_json NVARCHAR(MAX) NULL,
        business_key_hash CHAR(64) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        valid_from_utc DATETIME2(0) NOT NULL,
        valid_to_utc DATETIME2(0) NULL,
        first_seen_import_utc DATETIME2(0) NOT NULL,
        last_seen_import_utc DATETIME2(0) NOT NULL,
        is_current BIT NOT NULL,
        batch_id NVARCHAR(64) NOT NULL,
        source_file NVARCHAR(1024) NULL,
        source_sheet NVARCHAR(255) NULL,
        CONSTRAINT PK_ContractManagement_Header_hash PRIMARY KEY (business_key_hash, record_hash)
    );

    CREATE INDEX IX_ContractManagement_Header_current
        ON landing.ContractManagement_Header(is_current, valid_from_utc);

    CREATE INDEX IX_ContractManagement_Header_key_hash
        ON landing.ContractManagement_Header(business_key_hash, record_hash);

    CREATE INDEX IX_ContractManagement_Header_business
        ON landing.ContractManagement_Header(
            contract_number_m3,
            contract_number_customer,
            rentalposition_m3,
            row_type,
            wagon_number,
            customer
        );
END;
GO

IF OBJECT_ID('landing.ContractManagement_Batch', 'U') IS NULL
BEGIN
    CREATE TABLE landing.ContractManagement_Batch (
        contract_number_m3 NVARCHAR(120) NULL,
        contract_number_customer NVARCHAR(120) NULL,
        rentalposition_m3 NVARCHAR(120) NULL,
        remarks NVARCHAR(2000) NULL,
        row_type NVARCHAR(30) NULL,
        wagon_number NVARCHAR(120) NULL,
        customer NVARCHAR(200) NULL,
        row_json NVARCHAR(MAX) NULL,
        business_key_hash CHAR(64) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        valid_from_utc DATETIME2(0) NOT NULL,
        valid_to_utc DATETIME2(0) NULL,
        first_seen_import_utc DATETIME2(0) NOT NULL,
        last_seen_import_utc DATETIME2(0) NOT NULL,
        is_current BIT NOT NULL,
        batch_id NVARCHAR(64) NOT NULL,
        source_file NVARCHAR(1024) NULL,
        source_sheet NVARCHAR(255) NULL,
        CONSTRAINT PK_ContractManagement_Batch_hash PRIMARY KEY (business_key_hash, record_hash)
    );

    CREATE INDEX IX_ContractManagement_Batch_current
        ON landing.ContractManagement_Batch(is_current, valid_from_utc);
    CREATE INDEX IX_ContractManagement_Batch_key_hash
        ON landing.ContractManagement_Batch(business_key_hash, record_hash);
    CREATE INDEX IX_ContractManagement_Batch_business
        ON landing.ContractManagement_Batch(
            contract_number_m3,
            contract_number_customer,
            rentalposition_m3,
            row_type,
            wagon_number,
            customer
        );
END;
GO

IF OBJECT_ID('landing.ContractManagement_Wagon', 'U') IS NULL
BEGIN
    CREATE TABLE landing.ContractManagement_Wagon (
        contract_number_m3 NVARCHAR(120) NULL,
        contract_number_customer NVARCHAR(120) NULL,
        rentalposition_m3 NVARCHAR(120) NULL,
        remarks NVARCHAR(2000) NULL,
        row_type NVARCHAR(30) NULL,
        wagon_number NVARCHAR(120) NULL,
        customer NVARCHAR(200) NULL,
        row_json NVARCHAR(MAX) NULL,
        business_key_hash CHAR(64) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        valid_from_utc DATETIME2(0) NOT NULL,
        valid_to_utc DATETIME2(0) NULL,
        first_seen_import_utc DATETIME2(0) NOT NULL,
        last_seen_import_utc DATETIME2(0) NOT NULL,
        is_current BIT NOT NULL,
        batch_id NVARCHAR(64) NOT NULL,
        source_file NVARCHAR(1024) NULL,
        source_sheet NVARCHAR(255) NULL,
        CONSTRAINT PK_ContractManagement_Wagon_hash PRIMARY KEY (business_key_hash, record_hash)
    );

    CREATE INDEX IX_ContractManagement_Wagon_current
        ON landing.ContractManagement_Wagon(is_current, valid_from_utc);
    CREATE INDEX IX_ContractManagement_Wagon_key_hash
        ON landing.ContractManagement_Wagon(business_key_hash, record_hash);
    CREATE INDEX IX_ContractManagement_Wagon_business
        ON landing.ContractManagement_Wagon(
            contract_number_m3,
            contract_number_customer,
            rentalposition_m3,
            row_type,
            wagon_number,
            customer
        );
END;
GO
