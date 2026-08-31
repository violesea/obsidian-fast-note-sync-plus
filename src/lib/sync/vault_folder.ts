import type { Vault } from "obsidian";

export type VaultFolderCreateResult = "created" | "existing";

/**
 * Create a Vault folder with idempotent concurrent semantics.
 *
 * Obsidian documents Vault.createFolder() as throwing when the folder already
 * exists. During concurrent materialization, the adapter can already contain
 * the winning folder while Vault.getFolderByPath() still reflects an older
 * metadata-cache snapshot. Recheck the adapter after a rejected create and
 * swallow only the case where the real path is now a folder.
 */
export async function createVaultFolderIdempotent(
  vault: Vault,
  folderPath: string,
): Promise<VaultFolderCreateResult> {
  if (vault.getFolderByPath(folderPath) != null) return "existing";

  try {
    await vault.createFolder(folderPath);
    return "created";
  } catch (error) {
    let stat = null;
    try {
      stat = await vault.adapter.stat(folderPath);
    } catch {
      // Preserve the authoritative create error below.
    }
    if (stat?.type === "folder") return "existing";
    throw error;
  }
}
