import React, { useState, useEffect } from 'react';
import { Database, Plus, Trash2, CheckCircle, AlertTriangle, Loader2, Info, X, Search, ChevronLeft, ChevronRight } from 'lucide-react';
import { getDataSources, createDataSource, deleteDataSource, testDataSourceConnection } from '../api';
import { Modal } from '../components/Common';

const DataSourcesPage = () => {
  const [sources, setSources] = useState([]);
  const [filters, setFilters] = useState({ name: '', type: '' });
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const pageSize = 12;

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isDetailModalOpen, setIsDetailModalOpen] = useState(false);
  const [selectedSource, setSelectedSource] = useState(null);
  
  const [formData, setFormData] = useState({ 
      name: '', 
      description: '',
      type: 'mysql', 
      connection_details: {
          host: 'localhost',
          port: 3306,
          user: 'root',
          password: '',
          database: ''
      }
  });
  const [testStatus, setTestStatus] = useState(null); // null, 'testing', 'success', 'error'
  const [testMessage, setTestMessage] = useState('');

  const fetchSources = async () => {
    try {
      const params = {
          skip: (page - 1) * pageSize,
          limit: pageSize
      };
      if (filters.name) params.name = filters.name;
      if (filters.type) params.type = filters.type;
      
      const res = await getDataSources(params);
      setSources(res.data.data);
      setTotal(res.data.total);
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => { fetchSources(); }, [page, filters]);

  const handleFilterChange = (key, value) => {
      setFilters(prev => ({ ...prev, [key]: value }));
      setPage(1); // Reset to first page on filter change
  };

  const handleInputChange = (field, value) => {
      setFormData(prev => ({
          ...prev,
          connection_details: {
              ...prev.connection_details,
              [field]: value
          }
      }));
  };

  const handleTestConnection = async () => {
      setTestStatus('testing');
      setTestMessage('');
      try {
          // Construct payload for test
          const payload = {
              type: formData.type,
              ...formData.connection_details
          };
          const res = await testDataSourceConnection(payload);
          if (res.data.status === 'success') {
              setTestStatus('success');
              setTestMessage(res.data.message);
          } else {
              setTestStatus('error');
              setTestMessage(res.data.message);
          }
      } catch (err) {
          setTestStatus('error');
          setTestMessage(err.response?.data?.detail || 'Connection failed');
      }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      // Convert connection_details back to JSON string for storage
      const payload = {
          name: formData.name,
          description: formData.description,
          type: formData.type,
          connection_info: JSON.stringify(formData.connection_details)
      };
      await createDataSource(payload);
      setIsModalOpen(false);
      fetchSources();
      // Reset form
      setFormData({ 
          name: '', 
          description: '',
          type: 'mysql', 
          connection_details: {
            host: 'localhost',
            port: 3306,
            user: 'root',
            password: '',
            database: ''
        }
       });
       setTestStatus(null);
    } catch (err) {
      alert('Failed to create data source');
    }
  };

  const handleDelete = async (id) => {
    if (confirm('确认删除此数据源?')) {
      try {
        await deleteDataSource(id);
        // Refresh data from server only after successful deletion
        await fetchSources();
      } catch (err) {
        alert('删除失败');
        console.error(err);
      }
    }
  };
  
  const openDetailModal = (source) => {
      setSelectedSource(source);
      setIsDetailModalOpen(true);
  };

  const renderConnectionFields = () => {
      const { type } = formData;
      const { connection_details } = formData;
      
      if (type === 'csv') {
          return (
             <div>
                <label className="block text-sm font-medium text-slate-400 mb-1">文件路径</label>
                <input 
                  type="text" 
                  className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                  value={connection_details.path || ''}
                  onChange={e => handleInputChange('path', e.target.value)}
                  placeholder="/path/to/file.csv"
                />
             </div>
          );
      }

      if (type === 'mysql') {
          return (
              <>
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">主机</label>
                        <input 
                        type="text" 
                        className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                        value={connection_details.host}
                        onChange={e => handleInputChange('host', e.target.value)}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">端口</label>
                        <input 
                        type="number" 
                        className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                        value={connection_details.port}
                        onChange={e => handleInputChange('port', parseInt(e.target.value))}
                        />
                    </div>
                </div>
                <div>
                    <label className="block text-sm font-medium text-slate-400 mb-1">数据库</label>
                    <input 
                    type="text" 
                    className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                    value={connection_details.database}
                    onChange={e => handleInputChange('database', e.target.value)}
                    />
                </div>
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">用户名</label>
                        <input 
                        type="text" 
                        className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                        value={connection_details.user}
                        onChange={e => handleInputChange('user', e.target.value)}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">密码</label>
                        <input 
                        type="password" 
                        className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                        value={connection_details.password}
                        onChange={e => handleInputChange('password', e.target.value)}
                        />
                    </div>
                </div>
              </>
          );
      }
      
      if (type === 'minio') {
          return (
             <>
                <div>
                    <label className="block text-sm font-medium text-slate-400 mb-1">服务端点 (Endpoint)</label>
                    <input 
                    type="text" 
                    className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                    value={connection_details.endpoint || ''}
                    onChange={e => handleInputChange('endpoint', e.target.value)}
                    />
                </div>
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">Access Key</label>
                        <input 
                        type="text" 
                        className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                        value={connection_details.access_key || ''}
                        onChange={e => handleInputChange('access_key', e.target.value)}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">Secret Key</label>
                        <input 
                        type="password" 
                        className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                        value={connection_details.secret_key || ''}
                        onChange={e => handleInputChange('secret_key', e.target.value)}
                        />
                    </div>
                </div>
             </>
          );
      }

      return <div className="text-slate-500 text-sm">No specific configuration for this type.</div>;
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
          <Database className="text-blue-500" /> 数据源
        </h2>
        <button 
          onClick={() => setIsModalOpen(true)}
          className="bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded-md flex items-center gap-2 transition-colors"
        >
          <Plus size={18} /> 添加数据源
        </button>
      </div>

      <div className="bg-slate-900 border border-slate-700 rounded-lg p-4 flex gap-4 items-center">
          <div className="relative flex-1 max-w-xs">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" size={16} />
              <input 
                  type="text" 
                  placeholder="按名称搜索..." 
                  className="w-full bg-slate-950 border border-slate-700 rounded-md pl-9 pr-4 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                  value={filters.name}
                  onChange={e => handleFilterChange('name', e.target.value)}
              />
          </div>
          <div className="w-48">
              <select 
                  className="w-full bg-slate-950 border border-slate-700 rounded-md px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
                  value={filters.type}
                  onChange={e => handleFilterChange('type', e.target.value)}
              >
                  <option value="">所有类型</option>
                  <option value="mysql">MySQL</option>
                  <option value="minio">MinIO (S3)</option>
                  <option value="csv">CSV File</option>
              </select>
          </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {sources.map(source => (
          <div key={source.id} className="bg-slate-800/50 border border-slate-700 rounded-lg p-5 hover:border-blue-500/50 transition-colors relative group">
            <div className="flex justify-between items-start mb-4">
              <div>
                <h3 className="font-semibold text-lg text-slate-200">{source.name}</h3>
                <span className="text-xs text-slate-400 uppercase tracking-wider bg-slate-800 px-2 py-0.5 rounded border border-slate-700 mt-1 inline-block">
                  {source.type}
                </span>
              </div>
              <div className="flex gap-2">
                  <button 
                    onClick={() => openDetailModal(source)}
                    className="text-slate-500 hover:text-blue-400"
                    title="View Details"
                  >
                    <Info size={18} />
                  </button>
                  <button onClick={() => handleDelete(source.id)} className="text-slate-500 hover:text-rose-500">
                    <Trash2 size={18} />
                  </button>
              </div>
            </div>
            
            <div className="flex items-center gap-2 mt-4 pt-4 border-t border-slate-700/50">
                <div className="flex items-center gap-1.5 text-xs font-medium text-emerald-400 bg-emerald-900/20 px-2 py-1 rounded border border-emerald-900/50">
                    <CheckCircle size={12} /> 已连接
                </div>
                <span className="text-xs text-slate-500 ml-auto">
                    ID: {source.id}
                </span>
            </div>
          </div>
        ))}
      </div>

      {/* Pagination Controls */}
      <div className="flex justify-between items-center mt-6 border-t border-slate-800 pt-4">
        <span className="text-sm text-slate-400">
            显示 {sources.length > 0 ? (page - 1) * pageSize + 1 : 0} 到 {Math.min(page * pageSize, total)} 条，共 {total} 条
        </span>
        <div className="flex gap-2">
            <button 
                disabled={page === 1}
                onClick={() => setPage(p => Math.max(1, p - 1))}
                className="flex items-center gap-1 px-3 py-1.5 bg-slate-800 text-slate-300 rounded hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed text-sm transition-colors"
            >
                <ChevronLeft size={16} /> 上一页
            </button>
            <button 
                disabled={page * pageSize >= total}
                onClick={() => setPage(p => p + 1)}
                className="flex items-center gap-1 px-3 py-1.5 bg-slate-800 text-slate-300 rounded hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed text-sm transition-colors"
            >
                下一页 <ChevronRight size={16} />
            </button>
        </div>
      </div>

      {/* Add Source Modal */}
      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="添加数据源">
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-400 mb-1">名称</label>
            <input 
              type="text" 
              required
              className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
              value={formData.name}
              onChange={e => setFormData({...formData, name: e.target.value})}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-400 mb-1">数据类型描述</label>
            <input 
              type="text" 
              className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
              value={formData.description || ''}
              onChange={e => setFormData({...formData, description: e.target.value})}
              placeholder="例如: 文本数据、时序数据、图像数据"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-400 mb-1">类型</label>
            <select 
              className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
              value={formData.type}
              onChange={e => setFormData({...formData, type: e.target.value})}
            >
              <option value="mysql">MySQL</option>
              <option value="minio">MinIO (S3)</option>
              <option value="csv">CSV File</option>
            </select>
          </div>
          
          <div className="border-t border-slate-800 pt-4 mt-2 space-y-4">
              {renderConnectionFields()}
          </div>

          {/* Test Connection Status */}
          {testStatus && (
              <div className={`mt-2 p-3 rounded text-sm flex items-center gap-2 ${
                  testStatus === 'success' ? 'bg-emerald-900/30 text-emerald-400' : 
                  testStatus === 'error' ? 'bg-rose-900/30 text-rose-400' : 'bg-blue-900/30 text-blue-400'
              }`}>
                  {testStatus === 'testing' && <Loader2 size={16} className="animate-spin" />}
                  {testStatus === 'success' && <CheckCircle size={16} />}
                  {testStatus === 'error' && <AlertTriangle size={16} />}
                  {testMessage || (testStatus === 'testing' ? 'Testing connection...' : '')}
              </div>
          )}

          <div className="flex justify-between mt-6">
            <button 
                type="button" 
                onClick={handleTestConnection}
                disabled={testStatus === 'testing'}
                className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded disabled:opacity-50"
            >
                测试连接
            </button>
            <div className="flex gap-3">
                <button type="button" onClick={() => setIsModalOpen(false)} className="px-4 py-2 text-slate-400 hover:text-white">取消</button>
                <button type="submit" className="bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded">创建</button>
            </div>
          </div>
        </form>
      </Modal>

      {/* Detail Modal */}
      <Modal isOpen={isDetailModalOpen} onClose={() => setIsDetailModalOpen(false)} title="连接详情">
          {selectedSource && (
              <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                      <div>
                          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">源名称</label>
                          <div className="text-slate-200 font-medium">{selectedSource.name}</div>
                      </div>
                      <div>
                          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">类型</label>
                          <div className="text-slate-200 font-medium">{selectedSource.type}</div>
                      </div>
                  </div>
                  
                  <div>
                      <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">描述</label>
                      <div className="text-slate-200 font-medium">{selectedSource.description || '-'}</div>
                  </div>
                  
                  <div>
                      <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">连接配置</label>
                      <div className="bg-slate-950 rounded p-3 text-xs font-mono text-slate-400 overflow-auto max-h-48 border border-slate-800">
                         <pre>{JSON.stringify(JSON.parse(selectedSource.connection_info), null, 2)}</pre>
                      </div>
                  </div>

                  <div className="grid grid-cols-2 gap-4 pt-4 border-t border-slate-800">
                      <div>
                          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">创建时间</label>
                          <div className="text-slate-300 text-sm">{new Date(selectedSource.created_at).toLocaleString()}</div>
                      </div>
                      <div>
                          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">最后更新</label>
                          <div className="text-slate-300 text-sm">{new Date(selectedSource.updated_at).toLocaleString()}</div>
                      </div>
                  </div>
              </div>
          )}
      </Modal>
    </div>
  );
};

export default DataSourcesPage;
