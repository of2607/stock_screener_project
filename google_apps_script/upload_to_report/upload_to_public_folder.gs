/**
 * Google Apps Script Web App: 接收 multipart/form-data 上傳檔案，存到指定 Google Drive 資料夾
 * POST 參數：
 *   - folder_id: 目標資料夾ID
 *   - filename: 檔案名稱
 *   - file: 檔案內容 (multipart)
 *
 * 回傳：Success: <fileUrl> 或 Error: <message>
 */
function doPost(e) {
  try {
    var folderId = e.parameter.folder_id;
    var fileName = e.parameter.filename || 'uploaded_file';
    var filedata = e.parameter.filedata;
    if (!folderId) throw new Error('folder_id 不能為空');
    if (!filedata) throw new Error('filedata 不能為空');
    var folder = DriveApp.getFolderById(folderId);

    var blob = Utilities.newBlob(Utilities.base64Decode(filedata), undefined, fileName);

    // 有同名檔案則覆蓋內容（保留版本），否則新建
    var files = folder.getFilesByName(fileName);
    if (files.hasNext()) {
      var file = files.next();
      // 直接用 setContent(blob.getBytes()) 以二進位覆蓋
      file.setContent(blob.getBytes());
      return ContentService.createTextOutput('Success: ' + file.getUrl() + ' (updated, versioned)');
    } else {
      var newFile = folder.createFile(blob);
      return ContentService.createTextOutput('Success: ' + newFile.getUrl() + ' (created)');
    }
  } catch (err) {
    return ContentService.createTextOutput('Error: ' + err.message);
  }
}
