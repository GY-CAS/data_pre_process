import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// DataSources API
export const getDataSources = (params) => api.get('/datasources/', { params });
export const searchDataSources = (params) => api.get('/datasources/search', { params });
export const createDataSource = (data) => api.post('/datasources/', data);
export const deleteDataSource = (id) => api.delete(`/datasources/${id}`);
export const getDataSourceMetadata = (id) => api.get(`/datasources/${id}/metadata`);
export const testDataSourceConnection = (data) => api.post('/datasources/test-connection', data);

// Tasks API
export const getTasks = (params) => api.get('/tasks/', { params });
export const createTask = (data) => api.post('/tasks/', data);
export const deleteTasks = (ids) => api.delete('/tasks/', { data: ids });
export const deleteTask = (id) => api.delete(`/tasks/${id}`);
export const runTask = (id) => api.post(`/tasks/${id}/run`);
export const getTask = (id) => api.get(`/tasks/${id}`);

// Audit API
export const getAuditLogs = (params) => api.get('/audit/', { params });
export const deleteAuditLogs = (ids) => api.delete('/audit/', { data: ids });
export const createAuditLog = (data) => api.post('/audit/', data);

// Data Management API
export const getDataAssets = () => api.get('/data-mgmt/assets');
export const searchDataAssets = (params) => api.get('/data-mgmt/assets/search', { params });
export const deleteDataAsset = (name) => api.delete(`/data-mgmt/${name}`);
export const previewData = (path, limit = 20, offset = 0, id = null) => api.get('/data-mgmt/preview', { params: { path, limit, offset, id } });
export const getDataStructure = (path, id = null) => api.get('/data-mgmt/structure', { params: { path, id } });
export const updateTableRow = (table, rowId, data) => {
  const encodedTable = encodeURIComponent(String(table));
  const encodedRowId = encodeURIComponent(String(rowId));
  return api.put(`/data-mgmt/table/${encodedTable}/row/${encodedRowId}`, { row_id: String(rowId), data });
};
export const deleteTableRow = (table, rowId) => {
  const encodedTable = encodeURIComponent(String(table));
  const encodedRowId = encodeURIComponent(String(rowId));
  return api.delete(`/data-mgmt/table/${encodedTable}/row/${encodedRowId}`);
};
export const downloadDataAsset = (name, format = 'csv') => api.get(`/data-mgmt/download/${name}`, { params: { format }, responseType: 'blob' });
export const getCompleteAssets = () => api.get('/data-mgmt/assets-complete');

export default api;
