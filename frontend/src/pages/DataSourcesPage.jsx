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
          database: '',
          // Common fields, defaults will be adjusted on type change
      }
  });

  // Effect to reset/set default ports when type changes
  useEffect(() => {
     if (formData.type === 'clickhouse') {
         setFormData(prev => ({
             ...prev,
             connection_details: { ...prev.connection_details, port: 9000, user: 'default', database: 'default' }
         }));
     } else if (formData.type === 'mysql') {
         setFormData(prev => ({
             ...prev,
             connection_details: { ...prev.connection_details, port: 3306, user: 'root', database: '' }
         }));
     } else if (formData.type === 'minio') {
         setFormData(prev => ({
            ...prev,
            connection_details: { ...prev.connection_details, endpoint: 'http://localhost:9000', access_key: 'minioadmin', secret_key: 'minioadmin' }
         }));
     }
  }, [formData.type]);
  const [testStatus, setTestStatus] = useState(null); // null, 'testing', 'success', 'error'
  const [testMessage, setTestMessage] = useState('');

  const [connectionStatuses, setConnectionStatuses] = useState({}); // { id: { status: 'loading' | 'success' | 'error', message: '' } }
  
  // Duplicate Check State
  const [duplicateError, setDuplicateError] = useState('');
  const [isCheckingDuplicate, setIsCheckingDuplicate] = useState(false);

  useEffect(() => {
    const checkDuplicate = async () => {
        if (!formData.name.trim() || !isModalOpen) {
            setDuplicateError('');
            return;
        }
        
        setIsCheckingDuplicate(true);
        try {
            // Fetch potential matches
            const res = await getDataSources({ 
                name: formData.name, 
                type: formData.type,
                limit: 50 
            });
            
            // Backend performs 'contains' search, so we must check exact match
            const exists = res.data.data.some(s => s.name === formData.name && s.type === formData.type);
            
            if (exists) {
                setDuplicateError(`该类型 (${formData.type}) 下已存在名为 '${formData.name}' 的数据源`);
            } else {
                setDuplicateError('');
            }
        } catch (error) {
            console.error("Duplicate check failed", error);
        } finally {
            setIsCheckingDuplicate(false);
        }
    };

    const timer = setTimeout(checkDuplicate, 500);
    return () => clearTimeout(timer);
  }, [formData.name, formData.type, isModalOpen]);

  const fetchSources = async () => {
    try {
      const params = {
          skip: (page - 1) * pageSize,
          limit: pageSize
      };
      if (filters.name) params.name = filters.name;
      if (filters.type) params.type = filters.type;
      
      const res = await getDataSources(params);
      const data = res.data.data;
      setSources(data);
      setTotal(res.data.total);

      // Check connections for all loaded sources
      checkAllConnections(data);
    } catch (err) {
      console.error(err);
    }
  };

  const checkAllConnections = async (sourceList) => {
      // Initialize status to loading
      const initialStatus = {};
      sourceList.forEach(s => {
          initialStatus[s.id] = { status: 'loading', message: '' };
      });
      setConnectionStatuses(prev => ({ ...prev, ...initialStatus }));

      // Check each source
      // We can run these in parallel or sequence. Parallel is better for UI.
      sourceList.forEach(async (source) => {
          try {
              const connectionInfo = JSON.parse(source.connection_info);
              const payload = {
                  type: source.type,
                  ...connectionInfo
              };
              const res = await testDataSourceConnection(payload);
              
              if (res.data.status === 'success') {
                  setConnectionStatuses(prev => ({
                      ...prev,
                      [source.id]: { status: 'success', message: 'Connected' }
                  }));
              } else {
                  setConnectionStatuses(prev => ({
                      ...prev,
                      [source.id]: { status: 'error', message: res.data.message }
                  }));
              }
          } catch (err) {
               setConnectionStatuses(prev => ({
                  ...prev,
                  [source.id]: { status: 'error', message: err.response?.data?.detail || 'Connection failed' }
              }));
          }
      });
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

      if (type === 'mysql' || type === 'clickhouse') {
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
                        placeholder={type === 'clickhouse' ? "localhost" : "localhost"}
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">端口</label>
                        <input 
                        type="number" 
                        className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                        value={connection_details.port}
                        onChange={e => handleInputChange('port', parseInt(e.target.value))}
                        placeholder={type === 'clickhouse' ? "9000" : "3306"}
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
                    placeholder={type === 'clickhouse' ? "default" : "test_db"}
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
                        placeholder="root"
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

  const [jumpPage, setJumpPage] = useState('');

  const handleJump = () => {
      const p = parseInt(jumpPage);
      const totalPages = Math.ceil(total / pageSize);
      if (!isNaN(p) && p >= 1 && p <= totalPages) {
          setPage(p);
          setJumpPage('');
      }
  };

  return (
    <div className="space-y-6 flex flex-col h-[calc(100vh-8rem)]">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-slate-800 flex items-center gap-2">
          <Database className="text-blue-500" /> 数据源
        </h2>
        <button 
          onClick={() => {
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
              setDuplicateError('');
              setTestStatus(null);
              setIsModalOpen(true);
          }}
          className="bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded-md flex items-center gap-2 transition-colors shadow-sm"
        >
          <Plus size={18} /> 添加数据源
        </button>
      </div>

      <div className="bg-white border border-slate-200 rounded-lg p-4 flex gap-4 items-center shadow-sm">
          <div className="relative flex-1 max-w-xs">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={16} />
              <input 
                  type="text" 
                  placeholder="按名称搜索..." 
                  className="w-full bg-slate-50 border border-slate-200 rounded-md pl-9 pr-4 py-2 text-sm text-slate-700 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500/20"
                  value={filters.name}
                  onChange={e => handleFilterChange('name', e.target.value)}
              />
          </div>
          <div className="w-48">
              <select 
                  className="w-full bg-slate-50 border border-slate-200 rounded-md px-3 py-2 text-sm text-slate-700 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500/20"
                  value={filters.type}
                  onChange={e => handleFilterChange('type', e.target.value)}
              >
                  <option value="">所有类型</option>
                  <option value="mysql">MySQL</option>
                  <option value="minio">MinIO (S3)</option>
              </select>
          </div>
      </div>

      <div className="flex-1 overflow-y-auto">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {sources.map(source => (
          <div key={source.id} className="bg-white border border-slate-200 rounded-lg p-5 hover:border-blue-500 hover:shadow-md transition-all relative group">
            <div className="flex justify-between items-start mb-4">
              <div>
                <h3 className="font-semibold text-lg text-slate-800">{source.name}</h3>
                <span className="text-xs text-slate-500 uppercase tracking-wider bg-slate-100 px-2 py-0.5 rounded border border-slate-200 mt-1 inline-block font-medium">
                  {source.type}
                </span>
              </div>
              <div className="flex gap-2">
                  <button 
                    onClick={() => openDetailModal(source)}
                    className="text-slate-400 hover:text-blue-500 p-1 rounded hover:bg-blue-50 transition-colors"
                    title="View Details"
                  >
                    <Info size={18} />
                  </button>
                  <button onClick={() => handleDelete(source.id)} className="text-slate-400 hover:text-rose-500 p-1 rounded hover:bg-rose-50 transition-colors">
                    <Trash2 size={18} />
                  </button>
              </div>
            </div>
            
            <div className="flex items-center gap-2 mt-4 pt-4 border-t border-slate-100">
                {connectionStatuses[source.id]?.status === 'loading' && (
                    <div className="flex items-center gap-1.5 text-xs font-medium text-slate-500 bg-slate-50 px-2 py-1 rounded border border-slate-200">
                        <Loader2 size={12} className="animate-spin" /> 检测中...
                    </div>
                )}
                {connectionStatuses[source.id]?.status === 'success' && (
                    <div className="flex items-center gap-1.5 text-xs font-medium text-emerald-600 bg-emerald-50 px-2 py-1 rounded border border-emerald-100">
                        <CheckCircle size={12} /> 已连接
                    </div>
                )}
                {connectionStatuses[source.id]?.status === 'error' && (
                    <div className="flex items-center gap-1.5 text-xs font-medium text-rose-600 bg-rose-50 px-2 py-1 rounded border border-rose-100" title={connectionStatuses[source.id]?.message}>
                        <AlertTriangle size={12} /> 连接失败
                    </div>
                )}
                {!connectionStatuses[source.id] && (
                    <div className="flex items-center gap-1.5 text-xs font-medium text-slate-400 bg-slate-50 px-2 py-1 rounded border border-slate-100">
                        <Loader2 size={12} className="animate-spin" /> 等待检测...
                    </div>
                )}
                <span className="text-xs text-slate-400 ml-auto font-mono">
                    ID: {source.id}
                </span>
            </div>
          </div>
        ))}
      </div>
      </div>

      {/* Pagination Controls */}
      <div className="mt-auto pt-4 border-t border-slate-200 flex items-center justify-between">
        <span className="text-sm text-slate-500">
            共 {total} 条
        </span>
        <div className="flex gap-2 items-center">
             <div className="flex items-center gap-2 mr-4">
                  <span className="text-sm text-slate-500">前往</span>
                  <input 
                      type="number" 
                      min="1" 
                      max={Math.ceil(total / pageSize)}
                      className="w-12 h-8 text-center bg-white border border-slate-200 rounded text-sm text-slate-700 focus:outline-none focus:border-blue-500"
                      value={jumpPage}
                      onChange={e => setJumpPage(e.target.value)}
                      onKeyDown={e => e.key === 'Enter' && handleJump()}
                  />
                  <span className="text-sm text-slate-500">页</span>
              </div>
            <button 
                disabled={page === 1}
                onClick={() => setPage(p => Math.max(1, p - 1))}
                className="w-8 h-8 flex items-center justify-center rounded bg-white border border-slate-200 text-slate-600 hover:text-slate-800 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
            >
                <ChevronLeft size={16} />
            </button>
            
            <div className="flex items-center gap-1">
                  {Array.from({ length: Math.min(5, Math.ceil(total / pageSize)) }, (_, i) => {
                      const totalPages = Math.ceil(total / pageSize);
                      let p = page;
                      if (totalPages > 5) {
                          if (page <= 3) p = i + 1;
                          else if (page >= totalPages - 2) p = totalPages - 4 + i;
                          else p = page - 2 + i;
                      } else {
                          p = i + 1;
                      }
                      
                      return (
                          <button
                              key={p}
                              onClick={() => setPage(p)}
                              className={`w-8 h-8 rounded text-sm font-medium transition-colors ${
                                  page === p 
                                  ? 'bg-blue-600 text-white shadow-sm' 
                                  : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-50'
                              }`}
                          >
                              {p}
                          </button>
                      );
                  })}
            </div>

            <button 
                disabled={page * pageSize >= total}
                onClick={() => setPage(p => p + 1)}
                className="w-8 h-8 flex items-center justify-center rounded bg-white border border-slate-200 text-slate-600 hover:text-slate-800 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
            >
                <ChevronRight size={16} />
            </button>
        </div>
      </div>

      {/* Add Source Modal */}
      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="添加数据源">
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-400 mb-1">名称</label>
            <div className="relative">
                <input 
                  type="text" 
                  required
                  className={`w-full bg-slate-950 border rounded px-3 py-2 text-slate-200 focus:outline-none focus:ring-1 ${
                      duplicateError 
                      ? 'border-rose-500 focus:border-rose-500 focus:ring-rose-500/20' 
                      : 'border-slate-700 focus:border-blue-500 focus:ring-blue-500/20'
                  }`}
                  value={formData.name}
                  onChange={e => setFormData({...formData, name: e.target.value})}
                />
                {isCheckingDuplicate && (
                    <div className="absolute right-3 top-1/2 -translate-y-1/2">
                        <Loader2 size={14} className="animate-spin text-slate-500" />
                    </div>
                )}
            </div>
            {duplicateError && (
                <p className="text-xs text-rose-500 mt-1 flex items-center gap-1">
                    <AlertTriangle size={12} /> {duplicateError}
                </p>
            )}
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
              <option value="clickhouse">ClickHouse</option>
              <option value="minio">MinIO (S3)</option>
            </select>
          </div>
          
          <div className="border-t border-slate-800 pt-4 mt-2 space-y-4">
              {renderConnectionFields()}
          </div>

          {/* Test Connection Status */}
          {testStatus && (
              <div className={`mt-2 p-3 rounded text-sm flex items-start gap-2 max-h-32 overflow-y-auto ${
                  testStatus === 'success' ? 'bg-emerald-900/30 text-emerald-400' : 
                  testStatus === 'error' ? 'bg-rose-900/30 text-rose-400' : 'bg-blue-900/30 text-blue-400'
              }`}>
                  <div className="shrink-0 mt-0.5">
                    {testStatus === 'testing' && <Loader2 size={16} className="animate-spin" />}
                    {testStatus === 'success' && <CheckCircle size={16} />}
                    {testStatus === 'error' && <AlertTriangle size={16} />}
                  </div>
                  <div className="break-all whitespace-pre-wrap">
                    {testMessage || (testStatus === 'testing' ? 'Testing connection...' : '')}
                  </div>
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
                <button 
                    type="submit" 
                    disabled={!!duplicateError || isCheckingDuplicate}
                    className="bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded disabled:opacity-50 disabled:cursor-not-allowed"
                >
                    创建
                </button>
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
                      <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">连接状态</label>
                      <div className="flex items-center gap-2">
                        {connectionStatuses[selectedSource.id]?.status === 'success' && (
                            <span className="text-xs font-medium text-emerald-500 bg-emerald-900/30 px-2 py-1 rounded border border-emerald-900/50 flex items-center gap-1">
                                <CheckCircle size={12} /> 正常
                            </span>
                        )}
                        {connectionStatuses[selectedSource.id]?.status === 'error' && (
                            <div className="flex flex-col gap-1">
                                <span className="text-xs font-medium text-rose-500 bg-rose-900/30 px-2 py-1 rounded border border-rose-900/50 flex items-center gap-1 w-fit">
                                    <AlertTriangle size={12} /> 失败
                                </span>
                                <span className="text-xs text-rose-400 break-all">{connectionStatuses[selectedSource.id]?.message}</span>
                            </div>
                        )}
                        {(connectionStatuses[selectedSource.id]?.status === 'loading' || !connectionStatuses[selectedSource.id]) && (
                             <span className="text-xs font-medium text-slate-500 bg-slate-800 px-2 py-1 rounded border border-slate-700 flex items-center gap-1">
                                <Loader2 size={12} className="animate-spin" /> 检测中...
                            </span>
                        )}
                      </div>
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
