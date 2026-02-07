import axios from 'axios';

const API_BASE_URL = 'http://127.0.0.1:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const getDataSources = (params) => api.get('/datasources/', { params });
export const createDataSource = (data) => api.post('/datasources/', data);
export const deleteDataSource = (id) => api.delete(`/datasources/${id}`);
export const getDataSourceMetadata = (id) => api.get(`/datasources/${id}/metadata`);
export const testDataSourceConnection = (data) => api.post('/datasources/test-connection', data);

export const getTasks = (params) => api.get('/tasks/', { params });
export const createTask = (data) => api.post('/tasks/', data);
export const deleteTasks = (ids) => api.delete('/tasks/', { data: ids });
export const deleteTask = (id) => api.delete(`/tasks/${id}`);
export const runTask = (id) => api.post(`/tasks/${id}/run`);
export const getTask = (id) => api.get(`/tasks/${id}`);

// Audit
export const getAuditLogs = (params) => api.get('/audit/', { params });
export const deleteAuditLogs = (ids) => api.delete('/audit/', { data: ids });
export const createAuditLog = (data) => api.post('/audit/', data);

// Data Management
export const getDataAssets = () => api.get('/data-mgmt/assets');
export const deleteDataAsset = (name) => api.delete(`/data-mgmt/${name}`);
export const previewData = (path, limit = 20, offset = 0, id = null) => api.get('/data-mgmt/preview', { params: { path, limit, offset, id } });
export const getDataStructure = (path, id = null) => api.get('/data-mgmt/structure', { params: { path, id } });
export const updateTableRow = (table, rowId, data) => api.put(`/data-mgmt/table/${table}/row/${rowId}`, { row_id: rowId, data });
export const deleteTableRow = (table, rowId) => api.delete(`/data-mgmt/table/${table}/row/${rowId}`);
export const downloadDataAsset = (name, format = 'csv') => api.get(`/data-mgmt/download/${name}`, { params: { format }, responseType: 'blob' });
// MinIO download needs special handling if it returns JSON with links, but if it returns blob it's fine.
// The backend returns JSON for MinIO, Blob for others. 
// We might need to handle responseType dynamically or parse blob if JSON?
// Let's use 'blob' but check if type is json in caller.

export default api;
