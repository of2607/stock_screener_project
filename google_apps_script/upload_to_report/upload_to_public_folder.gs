/**
 * Google Apps Script Web App: 接收 multipart/form-data 上傳檔案，存到指定 Google Drive 資料夾
 * POST 參數：
 *   - folder_id: 目標資料夾ID
 *   - filename: 檔案名稱
 *   - filedata: 檔案內容 (base64 編碼)
 *   - convert_to_sheet: 是否轉換 CSV 為 Google Sheets (預設 'true')
 *   - keep_csv_backup: 是否保留原始 CSV 備份 (預設 'true')
 *
 * 回傳：JSON 格式包含 success, csvUrl, sheetUrl, sheetId, rowCount, executionTime
 * 
 * 所需權限：
 * - https://www.googleapis.com/auth/spreadsheets (建立和編輯 Sheets)
 * - https://www.googleapis.com/auth/drive (存取 Drive 資料夾和檔案)
 */

/**
 * 初始化函數 - 用於觸發權限請求
 * 首次部署後請在 Apps Script 編輯器中手動執行此函數一次以授予權限
 */
function initialize() {
  // 這個函數會觸發所需的權限授權
  var testFolder = DriveApp.getRootFolder();
  var testSheet = SpreadsheetApp.create('權限測試');
  DriveApp.getFileById(testSheet.getId()).setTrashed(true);
  Logger.log('權限授權完成！');
}

function doPost(e) {
  var startTime = new Date().getTime();
  
  try {
    var folderId = e.parameter.folder_id;
    var fileName = e.parameter.filename || 'uploaded_file';
    var filedata = e.parameter.filedata;
    var convertToSheet = (e.parameter.convert_to_sheet || 'true') === 'true';
    var keepCsvBackup = (e.parameter.keep_csv_backup || 'true') === 'true';
    
    if (!folderId) throw new Error('folder_id 不能為空');
    if (!filedata) throw new Error('filedata 不能為空');
    
    var folder = DriveApp.getFolderById(folderId);
    var blob = Utilities.newBlob(Utilities.base64Decode(filedata), undefined, fileName);
    
    var result = {
      success: true,
      csvUrl: null,
      sheetUrl: null,
      sheetId: null,
      rowCount: 0,
      executionTime: 0
    };
    
    // 判斷是否為 CSV 檔案且需要轉換
    var isCsv = fileName.toLowerCase().endsWith('.csv');
    
    if (isCsv && convertToSheet) {
      try {
        // 嘗試轉換為 Google Sheets
        var sheetResult = convertCsvToSheet(blob, fileName, folder, keepCsvBackup);
        result.sheetUrl = sheetResult.sheetUrl;
        result.sheetId = sheetResult.sheetId;
        result.rowCount = sheetResult.rowCount;
        result.csvUrl = sheetResult.csvUrl;
      } catch (convertError) {
        // 轉換失敗時回退到只儲存 CSV
        Logger.log('CSV 轉換失敗，回退到儲存原始檔案: ' + convertError.message);
        result.csvUrl = uploadCsvFile(blob, fileName, folder);
        result.warning = 'CSV 轉 Sheets 失敗: ' + convertError.message;
      }
    } else {
      // 非 CSV 或不需轉換，直接上傳
      var files = folder.getFilesByName(fileName);
      if (files.hasNext()) {
        var file = files.next();
        file.setContent(blob.getBytes());
        result.csvUrl = file.getUrl();
      } else {
        var newFile = folder.createFile(blob);
        result.csvUrl = newFile.getUrl();
      }
    }
    
    var endTime = new Date().getTime();
    result.executionTime = (endTime - startTime) / 1000; // 秒
    
    return ContentService.createTextOutput(JSON.stringify(result))
      .setMimeType(ContentService.MimeType.JSON);
    
  } catch (err) {
    var endTime = new Date().getTime();
    return ContentService.createTextOutput(JSON.stringify({
      success: false,
      error: err.message,
      executionTime: (endTime - startTime) / 1000
    })).setMimeType(ContentService.MimeType.JSON);
  }
}

/**
 * 將 CSV Blob 轉換為 Google Sheets
 * @param {Blob} blob - CSV 檔案 Blob
 * @param {string} fileName - 檔案名稱
 * @param {Folder} folder - 目標資料夾
 * @param {boolean} keepCsvBackup - 是否保留 CSV 備份
 * @return {Object} {sheetUrl, sheetId, rowCount, csvUrl}
 */
function convertCsvToSheet(blob, fileName, folder, keepCsvBackup) {
  var csvContent = blob.getDataAsString();
  
  // 解析 CSV
  var csvData = Utilities.parseCsv(csvContent, ',');
  
  if (!csvData || csvData.length === 0) {
    throw new Error('CSV 資料為空');
  }
  
  var sheetName = fileName.replace(/\.csv$/i, '');
  var numRows = csvData.length;
  var numCols = csvData[0].length;
  
  // 檢查是否超過 Google Sheets 限制 (1000萬儲存格)
  if (numRows * numCols > 10000000) {
    throw new Error('資料量超過 Google Sheets 上限 (1000萬儲存格)');
  }
  
  // 檢查是否已存在同名 Spreadsheet
  var files = folder.getFilesByName(sheetName);
  var spreadsheet, sheet;
  
  if (files.hasNext()) {
    // 更新現有 Sheets
    var file = files.next();
    try {
      spreadsheet = SpreadsheetApp.open(file);
      sheet = spreadsheet.getSheets()[0];
      sheet.clear();
    } catch (e) {
      // 如果檔案存在但不是 Spreadsheet，刪除後重建
      file.setTrashed(true);
      spreadsheet = SpreadsheetApp.create(sheetName);
      DriveApp.getFileById(spreadsheet.getId()).moveTo(folder);
      sheet = spreadsheet.getSheets()[0];
    }
  } else {
    // 建立新 Spreadsheet
    spreadsheet = SpreadsheetApp.create(sheetName);
    DriveApp.getFileById(spreadsheet.getId()).moveTo(folder);
    sheet = spreadsheet.getSheets()[0];
  }
  
  // 分批寫入資料 (每批 5000 行)
  var batchSize = 5000;
  for (var i = 0; i < numRows; i += batchSize) {
    var endRow = Math.min(i + batchSize, numRows);
    var batchData = csvData.slice(i, endRow);
    
    sheet.getRange(
      i + 1,
      1,
      batchData.length,
      numCols
    ).setValues(batchData);
    
    // 強制執行以避免超時
    if (i + batchSize < numRows) {
      SpreadsheetApp.flush();
    }
  }
  
  // 設定欄位格式（數字、文字、日期）
  applyColumnFormats(sheet, csvData);
  
  // 凍結標題列
  if (numRows > 1) {
    sheet.setFrozenRows(1);
  }
  
  var result = {
    sheetUrl: spreadsheet.getUrl(),
    sheetId: spreadsheet.getId(),
    rowCount: numRows,
    csvUrl: null
  };
  
  // 如果需要保留 CSV 備份
  if (keepCsvBackup) {
    result.csvUrl = uploadCsvFile(blob, fileName, folder);
  }
  
  return result;
}

/**
 * 根據欄位名稱判斷並套用格式（僅處理文字和日期欄位）
 * @param {Sheet} sheet - Google Sheets 工作表
 * @param {Array} csvData - CSV 資料陣列
 */
function applyColumnFormats(sheet, csvData) {
  if (csvData.length < 2) return; // 至少需要標題行和一行資料
  
  var headers = csvData[0];
  var dataStartRow = 2; // 資料從第 2 行開始（第 1 行是標題）
  var lastRow = csvData.length;
  
  for (var col = 0; col < headers.length; col++) {
    var header = String(headers[col]).toLowerCase().trim();
    var colIndex = col + 1; // Google Sheets 列索引從 1 開始
    var range = sheet.getRange(dataStartRow, colIndex, lastRow - 1, 1);
    
    // 只處理文字欄位和日期欄位
    if (isTextColumn(header)) {
      // 文字欄位：股票代號、收盤日等（保留前導零）
      range.setNumberFormat('@');
    } else if (isDateColumn(header)) {
      // 日期欄位
      range.setNumberFormat('yyyy-mm-dd');
    }
    // 其他欄位保持 Google Sheets 自動判斷的格式
  }
}

/**
 * 判斷是否為文字欄位（需保留前導零）
 */
function isTextColumn(header) {
  var textPatterns = [
    '代號', 'symbol', 'code', 'stock_code',
    '收盤日', '除息交易日', '股東會日期',
    '財報幣別', '幣別', 'currency',
    '財年', '年度', 'year',
    '產業別', 'sector', 'industry',
    '交易所', 'exchange',
    '國家', 'country',
    'company'
  ];
  
  return textPatterns.some(function(pattern) {
    return header.indexOf(pattern) !== -1;
  });
}

/**
 * 判斷是否為日期欄位
 */
function isDateColumn(header) {
  var datePatterns = [
    'date', '日期', '日',
    'created_at', 'updated_at'
  ];
  
  return datePatterns.some(function(pattern) {
    return header.indexOf(pattern) !== -1;
  });
}


/**
 * 上傳或更新 CSV 檔案
 * @param {Blob} blob - CSV 檔案 Blob
 * @param {string} fileName - 檔案名稱
 * @param {Folder} folder - 目標資料夾
 * @return {string} 檔案 URL
 */
function uploadCsvFile(blob, fileName, folder) {
  var files = folder.getFilesByName(fileName);
  if (files.hasNext()) {
    var file = files.next();
    file.setContent(blob.getBytes());
    return file.getUrl();
  } else {
    var newFile = folder.createFile(blob);
    return newFile.getUrl();
  }
}
