param(
    [string]$Server = "localhost",
    [string]$StaticDatabase = "tmdsDatabaseStatic",
    [string]$DynamicDatabase = "tmdsDatabaseDynamic",
    [string]$OutDir = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports\raw\sql_foundation")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Data

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

function Get-SafeName {
    param([string]$Value)
    return ($Value -replace "[^A-Za-z0-9._-]", "_")
}

function Export-QueryResult {
    param(
        [string]$Database,
        [string]$Name,
        [string]$Sql
    )

    $connectionString = "Data Source=$Server;Initial Catalog=$Database;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=True;Application Name=`"Codex TMDS Foundation Export`";Connect Timeout=5"
    $connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
    $connection.Open()
    try {
        $command = $connection.CreateCommand()
        $command.CommandText = $Sql
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
        $table = New-Object System.Data.DataTable
        [void]$adapter.Fill($table)

        $rows = New-Object System.Collections.Generic.List[object]
        foreach ($row in $table.Rows) {
            $entry = [ordered]@{}
            foreach ($column in $table.Columns) {
                $value = $row[$column.ColumnName]
                if ($value -is [System.DBNull]) {
                    $entry[$column.ColumnName] = $null
                } else {
                    $entry[$column.ColumnName] = $value
                }
            }
            $rows.Add([pscustomobject]$entry)
        }

        $safeBase = Get-SafeName "$Database.$Name"
        $jsonPath = Join-Path $OutDir "$safeBase.json"
        $csvPath = Join-Path $OutDir "$safeBase.csv"
        $metaPath = Join-Path $OutDir "$safeBase.meta.json"

        $rows | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $jsonPath
        $rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath

        [pscustomobject]@{
            database = $Database
            export_name = $Name
            row_count = $rows.Count
            json_path = $jsonPath
            csv_path = $csvPath
        } | ConvertTo-Json -Depth 4 | Set-Content -Encoding UTF8 -Path $metaPath

        return [pscustomobject]@{
            database = $Database
            export_name = $Name
            row_count = $rows.Count
            json_path = $jsonPath
            csv_path = $csvPath
            meta_path = $metaPath
        }
    } finally {
        $connection.Close()
    }
}

$componentLookupSql = @"
SET NOCOUNT ON;
SELECT
    component_family,
    component_uid,
    parent_control_point_uid,
    component_name,
    component_secondary_name,
    component_detail_name,
    component_codeline,
    territory_assignment,
    subdivision
FROM (
    SELECT
        'control_point' AS component_family,
        cp.UID AS component_uid,
        cp.ControlPoint AS parent_control_point_uid,
        cp.Name AS component_name,
        cp.UniqueName AS component_secondary_name,
        cp.PTC_SiteName AS component_detail_name,
        cp.Codeline AS component_codeline,
        cp.TerritoryAssignment AS territory_assignment,
        cp.Subdivision AS subdivision
    FROM dbo.tblCompControlPoints cp

    UNION ALL

    SELECT
        'signal' AS component_family,
        sig.UID AS component_uid,
        sig.ControlPoint AS parent_control_point_uid,
        sig.Name AS component_name,
        sig.ToolTipName AS component_secondary_name,
        sig.PTC_ProperName AS component_detail_name,
        sig.Codeline AS component_codeline,
        sig.TerritoryAssignment AS territory_assignment,
        sig.Subdivision AS subdivision
    FROM dbo.tblCompSignals sig

    UNION ALL

    SELECT
        'track' AS component_family,
        trk.UID AS component_uid,
        trk.ControlPoint AS parent_control_point_uid,
        trk.Name AS component_name,
        trk.TrkName AS component_secondary_name,
        trk.TrackNameAlias AS component_detail_name,
        trk.Codeline AS component_codeline,
        trk.TerritoryAssignment AS territory_assignment,
        trk.Subdivision AS subdivision
    FROM dbo.tblCompTracks trk

    UNION ALL

    SELECT
        'switch' AS component_family,
        sw.UID AS component_uid,
        sw.ControlPoint AS parent_control_point_uid,
        sw.Name AS component_name,
        sw.ToolTipName AS component_secondary_name,
        sw.PTC_ProperName AS component_detail_name,
        sw.Codeline AS component_codeline,
        sw.TerritoryAssignment AS territory_assignment,
        sw.Subdivision AS subdivision
    FROM dbo.tblCompSwitches sw

    UNION ALL

    SELECT
        'misc_device' AS component_family,
        misc.UID AS component_uid,
        misc.ControlPoint AS parent_control_point_uid,
        misc.Name AS component_name,
        misc.TypeOfDevice AS component_secondary_name,
        misc.SecondaryText AS component_detail_name,
        misc.Codeline AS component_codeline,
        misc.TerritoryAssignment AS territory_assignment,
        misc.Subdivision AS subdivision
    FROM dbo.tblCompMiscDevices misc
) component_lookup
ORDER BY component_family, parent_control_point_uid, component_uid;
"@

$codeBitLookupSql = @"
SET NOCOUNT ON;
SELECT
    WordType,
    CDLNNumber,
    CPNumber,
    CSNumber,
    BPAssignment,
    Mnemonic,
    LongName,
    BitType,
    UID
FROM dbo.tblCodeBitAssignments
ORDER BY CDLNNumber, CSNumber, CPNumber, BPAssignment, WordType;
"@

$codeLineContextSql = @"
SET NOCOUNT ON;
SELECT
    CodelineNumber,
    CodeLineName,
    CodeLineLimits,
    LegacyType,
    SessionProtocol,
    NormalCodeserverName,
    StandbyCodeserverName,
    PacketSwitchPName,
    PacketSwitchSName,
    PacketSwitchPIP,
    PacketSwitchSIP,
    PacketSwitchPPort,
    PacketSwitchSPort,
    SetupParameterString
FROM dbo.tblCodeLines
ORDER BY CodelineNumber;
"@

$codeLineDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblCodeLines
ORDER BY CodelineNumber;
"@

$codeStationContextSql = @"
SET NOCOUNT ON;
SELECT
    station.CodeLineNumber,
    station.CodeStationNumber,
    station.ControlPointNumber,
    station.StationName,
    station.ControlAddress,
    station.IndicationAddress,
    station.NumberOfControls,
    station.NumberOfIndications,
    station.WaysideATCSAddress,
    station.WaysideEMPAddress,
    station.NetworkPortalPrimary,
    station.NetworkPortalSecondary,
    station.NetworkPortalAddress,
    station.SetupParameterString AS StationSetupParameterString,
    line.CodeLineName,
    line.CodeLineLimits,
    line.LegacyType,
    line.SessionProtocol,
    line.NormalCodeserverName,
    line.StandbyCodeserverName,
    line.PacketSwitchPName,
    line.PacketSwitchSName,
    line.PacketSwitchPIP,
    line.PacketSwitchSIP,
    line.PacketSwitchPPort,
    line.PacketSwitchSPort,
    line.SetupParameterString AS CodeLineSetupParameterString,
    cp.Name AS ControlPointName,
    cp.PTC_SiteName AS PTCSiteName,
    cp.Subdivision AS SubdivisionUID,
    subd.Name AS SubdivisionName,
    cp.TerritoryAssignment
FROM dbo.tblCodeStations station
LEFT JOIN dbo.tblCodeLines line
    ON station.CodeLineNumber = line.CodelineNumber
LEFT JOIN dbo.tblCompControlPoints cp
    ON station.ControlPointNumber = cp.UID
LEFT JOIN dbo.tblSystemSubDivisions subd
    ON cp.Subdivision = subd.UID
ORDER BY station.CodeLineNumber, station.CodeStationNumber, station.ControlPointNumber;
"@

$codeStationDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblCodeStations
ORDER BY CodeLineNumber, CodeStationNumber, ControlPointNumber;
"@

$componentReferenceSql = @"
SET NOCOUNT ON;
SELECT
    component_family,
    component_uid,
    parent_control_point_uid,
    component_name,
    component_secondary_name,
    component_detail_name,
    component_codeline,
    reference_column,
    reference_value
FROM (
    SELECT
        'control_point' AS component_family,
        cp.UID AS component_uid,
        cp.ControlPoint AS parent_control_point_uid,
        cp.Name AS component_name,
        cp.UniqueName AS component_secondary_name,
        cp.PTC_SiteName AS component_detail_name,
        cp.Codeline AS component_codeline,
        refs.reference_column,
        refs.reference_value
    FROM dbo.tblCompControlPoints cp
    CROSS APPLY (VALUES
        ('CPIND1', cp.CPIND1), ('CPIND2', cp.CPIND2), ('CPIND3', cp.CPIND3), ('CPIND4', cp.CPIND4),
        ('CPIND5', cp.CPIND5), ('CPIND6', cp.CPIND6), ('CPIND7', cp.CPIND7), ('CPIND8', cp.CPIND8),
        ('CPIND9', cp.CPIND9), ('CPIND10', cp.CPIND10), ('CPIND11', cp.CPIND11), ('CPIND12', cp.CPIND12),
        ('CPIND13', cp.CPIND13), ('CPCTL1', cp.CPCTL1), ('CPCTL2', cp.CPCTL2), ('CPCTL3', cp.CPCTL3),
        ('CPCTL4', cp.CPCTL4), ('CPCTL5', cp.CPCTL5), ('CPCTL6', cp.CPCTL6), ('CPCTL7', cp.CPCTL7),
        ('CPCTL8', cp.CPCTL8), ('CPCTL9', cp.CPCTL9), ('CPCTL10', cp.CPCTL10), ('CPCTL11', cp.CPCTL11),
        ('CPCTL12', cp.CPCTL12), ('CPCTL13', cp.CPCTL13), ('ScadaAlarmReportingIndicationBit', cp.ScadaAlarmReportingIndicationBit),
        ('ExpSlotCPIND', cp.ExpSlotCPIND)
    ) refs(reference_column, reference_value)
    WHERE refs.reference_value IS NOT NULL AND LTRIM(RTRIM(refs.reference_value)) <> ''

    UNION ALL

    SELECT
        'signal' AS component_family,
        sig.UID AS component_uid,
        sig.ControlPoint AS parent_control_point_uid,
        sig.Name AS component_name,
        sig.ToolTipName AS component_secondary_name,
        sig.PTC_ProperName AS component_detail_name,
        sig.Codeline AS component_codeline,
        refs.reference_column,
        refs.reference_value
    FROM dbo.tblCompSignals sig
    CROSS APPLY (VALUES
        ('PIND', sig.PIND), ('PCTL', sig.PCTL), ('SIND', sig.SIND), ('SCTL', sig.SCTL),
        ('TIND', sig.TIND), ('TCTL', sig.TCTL), ('InTimeBit', sig.InTimeBit), ('BlockIndBit', sig.BlockIndBit),
        ('CallOnBit', sig.CallOnBit), ('FleetControlBit', sig.FleetControlBit),
        ('JointControlAuthorizationIndBit', sig.JointControlAuthorizationIndBit),
        ('JointControlAuthorizationCtlBit', sig.JointControlAuthorizationCtlBit),
        ('NearSideSignalBitControl', sig.NearSideSignalBitControl),
        ('NearSideSignaBitlIndication', sig.NearSideSignaBitlIndication),
        ('DynamicSignalRouteControlBitList', sig.DynamicSignalRouteControlBitList),
        ('DynamicSignalRouteCallOnBitList', sig.DynamicSignalRouteCallOnBitList)
    ) refs(reference_column, reference_value)
    WHERE refs.reference_value IS NOT NULL AND LTRIM(RTRIM(refs.reference_value)) <> ''

    UNION ALL

    SELECT
        'track' AS component_family,
        trk.UID AS component_uid,
        trk.ControlPoint AS parent_control_point_uid,
        trk.Name AS component_name,
        trk.TrkName AS component_secondary_name,
        trk.TrackNameAlias AS component_detail_name,
        trk.Codeline AS component_codeline,
        refs.reference_column,
        refs.reference_value
    FROM dbo.tblCompTracks trk
    CROSS APPLY (VALUES
        ('PIND', trk.PIND), ('PCTL', trk.PCTL), ('SIND', trk.SIND), ('SCTL', trk.SCTL),
        ('TIND', trk.TIND), ('TCTL', trk.TCTL), ('QIND', trk.QIND), ('QCTL', trk.QCTL),
        ('TrackLockBit', trk.TrackLockBit), ('DerailmentDetectorIndBit', trk.DerailmentDetectorIndBit),
        ('DerailmentDetectorCtlBit', trk.DerailmentDetectorCtlBit), ('TrackBlockingReferences', trk.TrackBlockingReferences)
    ) refs(reference_column, reference_value)
    WHERE refs.reference_value IS NOT NULL AND LTRIM(RTRIM(refs.reference_value)) <> ''

    UNION ALL

    SELECT
        'switch' AS component_family,
        sw.UID AS component_uid,
        sw.ControlPoint AS parent_control_point_uid,
        sw.Name AS component_name,
        sw.ToolTipName AS component_secondary_name,
        sw.PTC_ProperName AS component_detail_name,
        sw.Codeline AS component_codeline,
        refs.reference_column,
        refs.reference_value
    FROM dbo.tblCompSwitches sw
    CROSS APPLY (VALUES
        ('PIND', sw.PIND), ('PCTL', sw.PCTL), ('SIND', sw.SIND), ('SCTL', sw.SCTL),
        ('TIND', sw.TIND), ('TCTL', sw.TCTL), ('LockBit', sw.LockBit), ('SwitchBlkBit', sw.SwitchBlkBit),
        ('SwitchBlockControlBit', sw.SwitchBlockControlBit), ('RCPSControlBits', sw.RCPSControlBits),
        ('RCPSIndicationBits', sw.RCPSIndicationBits)
    ) refs(reference_column, reference_value)
    WHERE refs.reference_value IS NOT NULL AND LTRIM(RTRIM(refs.reference_value)) <> ''

    UNION ALL

    SELECT
        'misc_device' AS component_family,
        misc.UID AS component_uid,
        misc.ControlPoint AS parent_control_point_uid,
        misc.Name AS component_name,
        misc.TypeOfDevice AS component_secondary_name,
        misc.SecondaryText AS component_detail_name,
        misc.Codeline AS component_codeline,
        refs.reference_column,
        refs.reference_value
    FROM dbo.tblCompMiscDevices misc
    CROSS APPLY (VALUES
        ('CPIND1', misc.CPIND1), ('CPIND2', misc.CPIND2), ('CPIND3', misc.CPIND3),
        ('CPCTL1', misc.CPCTL1), ('CPCTL2', misc.CPCTL2), ('CPCTL3', misc.CPCTL3)
    ) refs(reference_column, reference_value)
    WHERE refs.reference_value IS NOT NULL AND LTRIM(RTRIM(refs.reference_value)) <> ''
) component_references
ORDER BY component_family, parent_control_point_uid, component_uid, reference_column;
"@

$controlPointDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblCompControlPoints
ORDER BY UID;
"@

$signalDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblCompSignals
ORDER BY UID;
"@

$trackDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblCompTracks
ORDER BY UID;
"@

$switchDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblCompSwitches
ORDER BY UID;
"@

$routeContextSql = @"
SET NOCOUNT ON;
SELECT
    route.SystemUID,
    route.RouteGUID,
    route.EntrySignal,
    entry_sig.Name AS EntrySignalName,
    entry_sig.ToolTipName AS EntrySignalToolTipName,
    route.ExitSignal,
    exit_sig.Name AS ExitSignalName,
    exit_sig.ToolTipName AS ExitSignalToolTipName,
    route.CPUID,
    cp.Name AS ControlPointName,
    route.SwitchList,
    route.Priority,
    route.IsEnabled
FROM dbo.tblSystemSignalRoutes route
LEFT JOIN dbo.tblCompSignals entry_sig
    ON route.EntrySignal = entry_sig.UID
LEFT JOIN dbo.tblCompSignals exit_sig
    ON route.ExitSignal = exit_sig.UID
LEFT JOIN dbo.tblCompControlPoints cp
    ON route.CPUID = cp.UID
ORDER BY route.CPUID, route.EntrySignal, route.ExitSignal, route.RouteGUID;
"@

$codeAssignmentContextSql = @"
SET NOCOUNT ON;
SELECT
    assignment.WordType,
    assignment.CDLNNumber,
    assignment.CSNumber,
    assignment.CPNumber,
    assignment.BPAssignment,
    assignment.Mnemonic,
    assignment.LongName,
    assignment.BitType,
    assignment.UID,
    line.CodeLineName,
    line.CodeLineLimits,
    line.LegacyType,
    line.SessionProtocol,
    line.NormalCodeserverName,
    line.StandbyCodeserverName,
    line.PacketSwitchPName,
    line.PacketSwitchSName,
    station.StationName,
    station.ControlAddress,
    station.IndicationAddress,
    station.NumberOfControls,
    station.NumberOfIndications,
    station.WaysideATCSAddress,
    station.WaysideEMPAddress,
    cp.Name AS ControlPointName,
    cp.PTC_SiteName AS PTCSiteName,
    cp.Subdivision AS SubdivisionUID,
    subd.Name AS SubdivisionName,
    cp.TerritoryAssignment
FROM dbo.tblCodeBitAssignments assignment
LEFT JOIN dbo.tblCodeLines line
    ON assignment.CDLNNumber = line.CodelineNumber
LEFT JOIN dbo.tblCodeStations station
    ON assignment.CDLNNumber = station.CodeLineNumber
   AND assignment.CSNumber = station.CodeStationNumber
   AND assignment.CPNumber = station.ControlPointNumber
LEFT JOIN dbo.tblCompControlPoints cp
    ON assignment.CPNumber = cp.UID
LEFT JOIN dbo.tblSystemSubDivisions subd
    ON cp.Subdivision = subd.UID
ORDER BY
    assignment.CDLNNumber,
    assignment.CSNumber,
    assignment.CPNumber,
    assignment.WordType,
    assignment.BPAssignment;
"@

$subdivisionContextSql = @"
SET NOCOUNT ON;
SELECT
    UID,
    Name,
    LongName,
    RailRoadName,
    BeginLimit,
    EndLimit,
    OfficeName,
    GeneralOrder,
    TimeTableDate,
    PTCUID,
    PTCDatabaseVersion,
    PTCDatabaseFileXmlUID,
    BackOfficeServerPrimary,
    BackOfficeServerSecondary,
    EnablePTCOperations,
    BOSDynamicStagingStatus,
    GTBNUMBER,
    GTBRANGE,
    GTBFACTOR,
    CurrentOperatingCircularUID
FROM dbo.tblSystemSubDivisions
ORDER BY UID;
"@

$miscDeviceContextSql = @"
SET NOCOUNT ON;
SELECT
    misc.UID,
    misc.ControlPoint,
    cp.Name AS ControlPointName,
    misc.Name,
    misc.Type,
    misc.TypeOfDevice,
    misc.DeviceCategory,
    misc.SecondaryText,
    misc.DeviceControl,
    misc.ControlOnly,
    misc.IndicateOnly,
    misc.FlashControl,
    misc.FlashIndicate,
    misc.ScadaMode,
    misc.TrackInterlockMode,
    misc.Codeline,
    misc.CPCTL1,
    misc.CPCTL2,
    misc.CPCTL3,
    misc.CPIND1,
    misc.CPIND2,
    misc.CPIND3,
    misc.TerritoryAssignment,
    misc.Subdivision,
    subd.Name AS SubdivisionName,
    misc.TrackGUID,
    trk.Name AS TrackComponentName,
    trk.TrkName AS TrackName,
    trk.TrackNameAlias AS TrackAlias,
    misc.Notes
FROM dbo.tblCompMiscDevices misc
LEFT JOIN dbo.tblCompControlPoints cp
    ON misc.ControlPoint = cp.UID
LEFT JOIN dbo.tblSystemSubDivisions subd
    ON misc.Subdivision = subd.UID
LEFT JOIN dbo.tblCompTracks trk
    ON TRY_CAST(misc.TrackGUID AS int) = trk.UID
ORDER BY misc.UID;
"@

$miscDeviceDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblCompMiscDevices
ORDER BY UID;
"@

$osEventContextSql = @"
SET NOCOUNT ON;
SELECT
    os.TrainSheetGUID,
    os.OsPoint,
    os.OsTime,
    os.TrackGUID,
    os.SubName,
    os.Direction,
    os.EventCreator,
    os.ReportData,
    trk.Name AS TrackComponentName,
    trk.TrkName AS TrackName,
    trk.TrackNameAlias AS TrackAlias,
    trk.RouteName AS TrackRouteName,
    trk.ControlPoint AS TrackControlPointUID,
    cp.Name AS TrackControlPointName
FROM dbo.tblTrainOsEventsActive os
LEFT JOIN [$StaticDatabase].dbo.tblCompTracks trk
    ON os.TrackGUID = trk.UID
LEFT JOIN [$StaticDatabase].dbo.tblCompControlPoints cp
    ON trk.ControlPoint = cp.UID
ORDER BY os.OsTime DESC, os.TrackGUID;
"@

$osEventDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblTrainOsEventsActive
ORDER BY OsTime DESC, TrackGUID;
"@

$activeTrainContextSql = @"
SET NOCOUNT ON;
SELECT
    train.Symbol,
    train.EngineID,
    train.Direction,
    train.TrainType,
    train.Origin,
    train.Dest,
    train.Subdivision,
    train.TrackGUID,
    trk.Name AS TrackComponentName,
    trk.TrkName AS TrackName,
    trk.TrackNameAlias AS TrackAlias,
    trk.RouteName AS TrackRouteName,
    cp.Name AS ControlPointName,
    train.TrainGUID,
    train.TrainLink,
    train.ScheduleStatus,
    train.PTCStatus,
    train.BulletinRouteCode,
    train.AuthorityDesignation,
    train.CTCAuthorityDesignation,
    train.HomeRoadCode,
    train.ActiveSubTrainSheet
FROM dbo.tblTrainsActive train
LEFT JOIN [$StaticDatabase].dbo.tblCompTracks trk
    ON train.TrackGUID = trk.UID
LEFT JOIN [$StaticDatabase].dbo.tblCompControlPoints cp
    ON trk.ControlPoint = cp.UID
ORDER BY train.Symbol, train.TrainGUID;
"@

$activeTrainDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblTrainsActive
ORDER BY Symbol, TrainGUID;
"@

$locomotiveRuntimeContextSql = @"
SET NOCOUNT ON;
WITH latest_position AS (
    SELECT
        pos.*,
        ROW_NUMBER() OVER (
            PARTITION BY LTRIM(RTRIM(pos.loco_id))
            ORDER BY pos.last_updated DESC, pos.id DESC
        ) AS row_rank
    FROM dbo.tblLocomotivePositionReport pos
),
latest_mdm AS (
    SELECT
        mdm.*,
        ROW_NUMBER() OVER (
            PARTITION BY LTRIM(RTRIM(mdm.LocomotiveId))
            ORDER BY mdm.Last2100 DESC, mdm.Id DESC
        ) AS row_rank
    FROM dbo.MdmTrainObjectData mdm
),
latest_departure AS (
    SELECT
        dep.*,
        ROW_NUMBER() OVER (
            PARTITION BY LTRIM(RTRIM(dep.LocoId))
            ORDER BY dep.DepartureTestTimeStamp DESC, dep.Id DESC
        ) AS row_rank
    FROM dbo.tblBosDepartureTestData dep
)
SELECT
    pos.id AS PositionId,
    LTRIM(RTRIM(pos.loco_id)) AS LocoId,
    pos.scac,
    pos.train_symbol,
    pos.last_updated,
    pos.reason_for_report,
    pos.locomotive_state_summary,
    pos.locomotive_state,
    pos.control_brake,
    pos.speed,
    pos.head_end_latitude,
    pos.head_end_longitude,
    pos.head_end_track_name,
    pos.head_end_milepost,
    pos.head_end_milepost_prefix,
    pos.head_end_milepost_suffix,
    mdm.Id AS MdmId,
    mdm.Last2100,
    mdm.IcdInterfaceVersion,
    departure.Id AS DepartureTestId,
    departure.DepartureTestStatus,
    departure.OnboardSoftwareVersion,
    departure.DepartureTestTimeStamp,
    departure.LocationName AS DepartureLocationName,
    departure.TrackName AS DepartureTrackName,
    departure.LocoStateSummary AS DepartureLocoStateSummary
FROM latest_position pos
LEFT JOIN latest_mdm mdm
    ON LTRIM(RTRIM(pos.loco_id)) = LTRIM(RTRIM(mdm.LocomotiveId))
   AND mdm.row_rank = 1
LEFT JOIN latest_departure departure
    ON LTRIM(RTRIM(pos.loco_id)) = LTRIM(RTRIM(departure.LocoId))
   AND departure.row_rank = 1
WHERE pos.row_rank = 1
ORDER BY pos.last_updated DESC, LocoId;
"@

$locomotivePositionDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblLocomotivePositionReport
ORDER BY last_updated DESC, id DESC;
"@

$mdmTrainObjectDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.MdmTrainObjectData
ORDER BY Last2100 DESC, Id DESC;
"@

$bosDepartureTestDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblBosDepartureTestData
ORDER BY DepartureTestTimeStamp DESC, Id DESC;
"@

$authorityComponentContextSql = @"
SET NOCOUNT ON;
WITH component_lookup AS (
    SELECT 'control_point' AS component_family, UID AS component_uid, Name AS component_name, UniqueName AS component_secondary_name
    FROM [$StaticDatabase].dbo.tblCompControlPoints
    UNION ALL
    SELECT 'signal', UID, Name, ToolTipName
    FROM [$StaticDatabase].dbo.tblCompSignals
    UNION ALL
    SELECT 'track', UID, Name, TrkName
    FROM [$StaticDatabase].dbo.tblCompTracks
    UNION ALL
    SELECT 'switch', UID, Name, ToolTipName
    FROM [$StaticDatabase].dbo.tblCompSwitches
)
SELECT
    auth.AuthorityUID,
    auth.AuthorityNumber,
    auth.AuthorityType,
    auth.Direction,
    auth.TrainSymbol,
    auth.IssueTo,
    auth.AuthorityLimits,
    auth.Components,
    auth.TerritoryAssignment,
    auth.PTCAuthorityStatus,
    auth.PTCAuthorityText,
    TRY_CAST(token.component_uid_token AS int) AS ComponentUID,
    component_lookup.component_family,
    component_lookup.component_name,
    component_lookup.component_secondary_name
FROM dbo.tblAuthoritiesActive auth
CROSS APPLY (
    SELECT TRY_CAST('<x>' + REPLACE(COALESCE(auth.Components, ''), '|', '</x><x>') + '</x>' AS xml) AS token_xml
) split_source
CROSS APPLY (
    SELECT LTRIM(RTRIM(node.value('.', 'nvarchar(100)'))) AS component_uid_token
    FROM split_source.token_xml.nodes('/x') token(node)
) token
LEFT JOIN component_lookup
    ON TRY_CAST(token.component_uid_token AS int) = component_lookup.component_uid
WHERE token.component_uid_token <> ''
ORDER BY auth.AuthorityNumber, ComponentUID;
"@

$authorityDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblAuthoritiesActive
ORDER BY AuthorityNumber, AuthorityUID;
"@

$bulletinComponentContextSql = @"
SET NOCOUNT ON;
WITH component_lookup AS (
    SELECT 'control_point' AS component_family, UID AS component_uid, Name AS component_name, UniqueName AS component_secondary_name
    FROM [$StaticDatabase].dbo.tblCompControlPoints
    UNION ALL
    SELECT 'signal', UID, Name, ToolTipName
    FROM [$StaticDatabase].dbo.tblCompSignals
    UNION ALL
    SELECT 'track', UID, Name, TrkName
    FROM [$StaticDatabase].dbo.tblCompTracks
    UNION ALL
    SELECT 'switch', UID, Name, ToolTipName
    FROM [$StaticDatabase].dbo.tblCompSwitches
)
SELECT
    bulletin.RestrictionUID,
    bulletin.RestrictionType,
    bulletin.SubType,
    bulletin.RestrictionSubName,
    bulletin.TrackName,
    bulletin.TrackNameList,
    bulletin.Direction,
    bulletin.Information,
    bulletin.CompList,
    bulletin.PTCBulletinText,
    TRY_CAST(token.component_uid_token AS int) AS ComponentUID,
    component_lookup.component_family,
    component_lookup.component_name,
    component_lookup.component_secondary_name
FROM dbo.tblSystemBulletins bulletin
CROSS APPLY (
    SELECT TRY_CAST('<x>' + REPLACE(COALESCE(bulletin.CompList, ''), '-', '</x><x>') + '</x>' AS xml) AS token_xml
) split_source
CROSS APPLY (
    SELECT LTRIM(RTRIM(node.value('.', 'nvarchar(100)'))) AS component_uid_token
    FROM split_source.token_xml.nodes('/x') token(node)
) token
LEFT JOIN component_lookup
    ON TRY_CAST(token.component_uid_token AS int) = component_lookup.component_uid
WHERE token.component_uid_token <> ''
ORDER BY bulletin.RestrictionUID, ComponentUID;
"@

$bulletinDetailFullSql = @"
SET NOCOUNT ON;
SELECT *
FROM dbo.tblSystemBulletins
ORDER BY RestrictionUID;
"@

$bosEmpMessagesSql = @"
SET NOCOUNT ON;
SELECT
    Id,
    [Key],
    Ttl,
    CASE
        WHEN CHARINDEX('-', [Key]) > 0 THEN LEFT([Key], CHARINDEX('-', [Key]) - 1)
        ELSE NULL
    END AS MessagePrefix,
    CASE
        WHEN CHARINDEX('-', [Key]) > 0
         AND CHARINDEX('-', [Key], CHARINDEX('-', [Key]) + 1) > CHARINDEX('-', [Key])
        THEN SUBSTRING(
            [Key],
            CHARINDEX('-', [Key]) + 1,
            CHARINDEX('-', [Key], CHARINDEX('-', [Key]) + 1) - CHARINDEX('-', [Key]) - 1
        )
        ELSE NULL
    END AS EmpAddress
FROM dbo.tblBosTemporaryEmpMessageHistory
ORDER BY Ttl DESC, Id DESC;
"@

$manifest = New-Object System.Collections.Generic.List[object]
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "component_lookup" -Sql $componentLookupSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "code_bit_lookup" -Sql $codeBitLookupSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "code_line_context" -Sql $codeLineContextSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "code_line_detail_full" -Sql $codeLineDetailFullSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "code_station_context" -Sql $codeStationContextSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "code_station_detail_full" -Sql $codeStationDetailFullSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "code_assignment_context" -Sql $codeAssignmentContextSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "component_reference_rows" -Sql $componentReferenceSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "control_point_detail_full" -Sql $controlPointDetailFullSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "signal_detail_full" -Sql $signalDetailFullSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "track_detail_full" -Sql $trackDetailFullSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "switch_detail_full" -Sql $switchDetailFullSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "route_context" -Sql $routeContextSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "subdivision_context" -Sql $subdivisionContextSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "misc_device_context" -Sql $miscDeviceContextSql))
$manifest.Add((Export-QueryResult -Database $StaticDatabase -Name "misc_device_detail_full" -Sql $miscDeviceDetailFullSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "active_train_detail_full" -Sql $activeTrainDetailFullSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "active_train_context" -Sql $activeTrainContextSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "locomotive_position_detail_full" -Sql $locomotivePositionDetailFullSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "mdm_train_object_detail_full" -Sql $mdmTrainObjectDetailFullSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "bos_departure_test_detail_full" -Sql $bosDepartureTestDetailFullSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "locomotive_runtime_context" -Sql $locomotiveRuntimeContextSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "os_event_detail_full" -Sql $osEventDetailFullSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "os_event_context" -Sql $osEventContextSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "authority_detail_full" -Sql $authorityDetailFullSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "authority_component_context" -Sql $authorityComponentContextSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "bulletin_detail_full" -Sql $bulletinDetailFullSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "bulletin_component_context" -Sql $bulletinComponentContextSql))
$manifest.Add((Export-QueryResult -Database $DynamicDatabase -Name "bos_emp_messages" -Sql $bosEmpMessagesSql))

$manifestPath = Join-Path $OutDir "sql_foundation_manifest.json"
$manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $manifestPath
Write-Host $manifestPath
