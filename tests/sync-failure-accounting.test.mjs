import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const read = (name) => fs.readFileSync(path.join(root, "src", "lib", "sync", name), "utf8");
const fileSource = read("operator_file.ts");
const noteSource = read("operator_note.ts");
const configSource = read("operator_config.ts");

const sliceFunction = (source, startMarker, endMarker) => {
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(start >= 0 && end > start, `${startMarker} must be locatable`);
  return source.slice(start, end);
};

// Contract: policy or cooldown skips that leave a server-declared attachment
// absent locally are failure-accounted before the page item is completed.
const fileUpdate = sliceFunction(fileSource, "export const receiveFileSyncUpdate", "export const receiveFileSyncDelete");
assert.match(fileUpdate, /isLargeBinarySyncRisk\(data\.size, plugin\)[\s\S]*?fileSyncTasks\.failed\+\+[\s\S]*?recordSyncCompleted\('file'/);
assert.match(fileUpdate, /isDownloadCoolingDown\(plugin, data\.path\)[\s\S]*?fileSyncTasks\.failed\+\+[\s\S]*?recordSyncCompleted\('file'/);

const downloadComplete = sliceFunction(fileSource, "const handleFileChunkDownloadComplete", "export const receiveFileRenameAck");
assert.match(downloadComplete, /isLargeBinarySyncRisk\(session\.size, plugin\)[\s\S]*?cleanupFileDownloadSession\(plugin, session, true\)/);

const fileMtime = sliceFunction(fileSource, "export const receiveFileSyncMtime", "export const receiveFileSyncChunkDownload");
assert.match(fileMtime, /isLargeBinarySyncRisk\(file\.stat\.size, plugin\)[\s\S]*?fileSyncTasks\.failed\+\+/);
assert.match(fileMtime, /\} else \{\s*plugin\.fileSyncTasks\.failed\+\+/);

// Contract: requesting RePush is not materialization. No hash baseline may be
// fabricated for missing bytes, and the round must remain uncommittable.
const fileRename = sliceFunction(fileSource, "export const receiveFileSyncRename", "const handleFileChunkDownloadComplete");
const fileRePushPos = fileRename.indexOf('SendMessage("FileRePush"');
assert.ok(fileRePushPos >= 0, "FileRePush branch must be locatable");
const fileRePush = fileRename.slice(fileRePushPos);
assert.match(fileRePush, /fileSyncTasks\.failed\+\+/);
assert.doesNotMatch(fileRePush.slice(0, fileRePush.indexOf("}, { maxRetries")), /setFileHash\(/);

const noteRename = sliceFunction(noteSource, "export const receiveNoteSyncRename", "export const receiveNoteModifyAck");
const noteRePushPos = noteRename.indexOf('SendMessage("NoteRePush"');
assert.ok(noteRePushPos >= 0, "NoteRePush branch must be locatable");
const noteRePush = noteRename.slice(noteRePushPos);
assert.match(noteRePush, /noteSyncTasks\.failed\+\+/);
assert.doesNotMatch(noteRePush.slice(0, noteRePush.indexOf("}, { maxRetries")), /setFileHash\(/);

// Contract: local conflicts preserve user data but fail the remote projection,
// preventing the whole-round watermark from moving past the unresolved item.
const noteModify = sliceFunction(noteSource, "export const receiveNoteSyncModify", "export const repairSuspiciousEmptyNotes");
assert.match(noteModify, /NoteModifyConflict[\s\S]*?noteSyncTasks\.failed\+\+[\s\S]*?recordSyncCompleted\('note'/);

const configModify = sliceFunction(configSource, "export const receiveConfigSyncModify", "export const receiveConfigUpload");
assert.match(configModify, /ConfigModifyConflict[\s\S]*?configSyncTasks\.failed\+\+[\s\S]*?recordSyncCompleted\('setting'/);
assert.match(configModify, /handleReceivedUpdate[\s\S]*?configSyncTasks\.failed\+\+[\s\S]*?recordSyncCompleted\('setting'/);

const folderSource = read("operator_folder.ts");
const folderDelete = sliceFunction(folderSource, "export const receiveFolderSyncDelete", "export const receiveFolderSyncRename");
assert.match(folderDelete, /FolderDeleteSkipped[\s\S]*?folderSyncTasks\.failed\+\+/);

console.log("sync-failure-accounting.test.mjs: all scenarios passed");
