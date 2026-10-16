const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  saveUrl: (url) => ipcRenderer.invoke('save-url', url),
});
