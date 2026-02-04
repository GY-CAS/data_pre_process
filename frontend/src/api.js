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
export const deleteTask = (id) => api.delete(`/tasks/${id}`);
export const runTask = (id) => api.post(`/tasks/${id}/run`);
export const getTask = (id) => api.get(`/tasks/${id}`);

// Audit
export const getAuditLogs = (params) => api.get('/audit/', { params });
export const createAuditLog = (data) => api.post('/audit/', data);

// Data Management
export const getDataAssets = () => api.get('/data-mgmt/assets');
export const deleteDataAsset = (name) => api.delete(`/data-mgmt/${name}`);
export const previewData = (path, limit = 20, offset = 0) => api.get('/data-mgmt/preview', { params: { path, limit, offset } });
export const getDataStructure = (path) => api.get('/data-mgmt/structure', { params: { path } });
export const updateTableRow = (table, rowId, data) => api.put(`/data-mgmt/table/${table}/row/${rowId}`, { row_id: rowId, data });
export const deleteTableRow = (table, rowId) => api.delete(`/data-mgmt/table/${table}/row/${rowId}`);

export default api;
