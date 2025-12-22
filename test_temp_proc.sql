
CREATE PROCEDURE dbo.test_wildcard_proc
AS
BEGIN
    -- First create source temp table
    SELECT 'A' AS ColA, 'B' AS ColB, 'C' AS ColC
    INTO #STEP1

    -- Now copy with wildcard
    SELECT
        offer.* --all attributes from previous step
        , 'D' AS ColD
    INTO #STEP2
    FROM #STEP1 AS offer
END
