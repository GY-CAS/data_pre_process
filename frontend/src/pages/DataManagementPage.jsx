import React, { useState, useEffect, useCallback } from 'react';
import { Folder, Eye, FileText, Download, Table as TableIcon, Database, Trash2, Edit2, Check, X, ChevronLeft, ChevronRight, ChevronDown, ChevronUp, Loader2, AlertTriangle, ArrowUp, ArrowDown } from 'lucide-react';
import { searchDataAssets, deleteDataAsset, previewData, getDataStructure, updateTableRow, deleteTableRow, downloadDataAsset } from '../api';
import { Modal } from '../components/Common';
import { Search, Filter } from 'lucide-react';

const DATA_TYPE_OPTIONS = [
  { value: 'TEXT', label: '文本数据' },
  { value: 'TIMESERIES', label: '时序数据' },
  { value: 'IMAGE', label: '图像数据' }
];

const ASSET_TYPE_OPTIONS = [
  { value: 'table', label: '数据库表' },
  { value: 'bucket', label: 'MinIO 存储桶' },
  { value: 'file', label: '本地文件' }
];

const PAGE_SIZE_OPTIONS = [10, 20, 50, 100];
const PREVIEW_PAGE_SIZE_OPTIONS = [20, 50, 100, 200];

const DataManagementPage = () => {
  const [assets, setAssets] = useState([]);
  const [filteredAssets, setFilteredAssets] = useState([]);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [previewContent, setPreviewContent] = useState(null);
  const [minioLinks, setMinioLinks] = useState(null);
  const [structureContent, setStructureContent] = useState(null);
  const [modalType, setModalType] = useState(null);
  const [exportFormat, setExportFormat] = useState('csv');
  
  const [filters, setFilters] = useState({ name: '', type: '', data_type: '' });
  const [isFilterExpanded, setIsFilterExpanded] = useState(false);
  const [searchTimeout, setSearchTimeout] = useState(null);

  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [total, setTotal] = useState(0);
  
  const [previewPage, setPreviewPage] = useState(1);
  const [previewPageSize, setPreviewPageSize] = useState(20);
  const [previewTotal, setPreviewTotal] = useState(0);
  const [previewTotalPages, setPreviewTotalPages] = useState(0);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState(null);
  const [sortField, setSortField] = useState(null);
  const [sortOrder, setSortOrder] = useState(null);

  const [editingRowId, setEditingRowId] = useState(null);
  const [editData, setEditData] = useState({});

  useEffect(() => {
    const savedFilters = localStorage.getItem('datamanagement_filters');
    const savedPageSize = localStorage.getItem('datamanagement_pageSize');
    const savedPreviewPageSize = localStorage.getItem('datamanagement_previewPageSize');
    
    if (savedFilters) {
      try {
        const parsed = JSON.parse(savedFilters);
        setFilters(parsed);
      } catch (e) {
        console.error('Failed to parse saved filters', e);
      }
    }
    if (savedPageSize) {
      setPageSize(parseInt(savedPageSize));
    }
    if (savedPreviewPageSize) {
      setPreviewPageSize(parseInt(savedPreviewPageSize));
    }
  }, []);

  useEffect(() => {
    localStorage.setItem('datamanagement_filters', JSON.stringify(filters));
  }, [filters]);

  useEffect(() => {
    localStorage.setItem('datamanagement_pageSize', pageSize.toString());
  }, [pageSize]);

  useEffect(() => {
    localStorage.setItem('datamanagement_previewPageSize', previewPageSize.toString());
  }, [previewPageSize]);

  const fetchAssets = useCallback(async () => {
    try {
      const params = {};
      if (filters.name) params.name = filters.name;
      if (filters.type) params.type = filters.type;
      if (filters.data_type) params.data_type = filters.data_type;
      
      const res = await searchDataAssets(params);
      setAssets(res.data.data || []);
      setTotal(res.data.total || 0);
    } catch (err) {
      console.error(err);
    }
  }, [filters]);

  useEffect(() => {
    fetchAssets();
  }, [fetchAssets]);

  useEffect(() => {
      let result = assets;
      
      if (filters.name) {
          result = result.filter(a => a.name.toLowerCase().includes(filters.name.toLowerCase()));
      }
      if (filters.type) {
          result = result.filter(a => a.type === filters.type);
      }
      if (filters.data_type) {
          result = result.filter(a => a.data_type === filters.data_type);
      }
      
      setTotal(result.length);
      const start = (page - 1) * pageSize;
      const end = start + pageSize;
      setFilteredAssets(result.slice(start, end));
  }, [assets, filters, page, pageSize]);

  const handleFilterChange = (key, value) => {
      if (searchTimeout) {
        clearTimeout(searchTimeout);
      }
      
      const newTimeout = setTimeout(() => {
        setFilters(prev => ({ ...prev, [key]: value }));
        setPage(1);
      }, key === 'name' ? 300 : 0);
      
      setSearchTimeout(newTimeout);
  };

  const clearFilters = () => {
    setFilters({ name: '', type: '', data_type: '' });
    setPage(1);
  };

  const activeFilterCount = [filters.name, filters.type, filters.data_type].filter(Boolean).length;

  const handleDeleteAsset = async (asset) => {
      if (confirm(`确认删除 ${asset.name}? 此操作不可撤销。`)) {
          try {
              const identifier = asset.id ? asset.id.toString() : asset.name;
              await deleteDataAsset(identifier);
              fetchAssets();
          } catch (err) {
              alert('删除失败: ' + (err.response?.data?.detail || err.message));
          }
      }
  };

  const handlePreview = useCallback(async (asset, pageNum = 1, pPageSize = previewPageSize, pSortField = sortField, pSortOrder = sortOrder) => {
    setPreviewLoading(true);
    setPreviewError(null);
    
    try {
      const res = await previewData(asset.path, pageNum, pPageSize, pSortField, pSortOrder, asset.id);
      setPreviewContent(res.data);
      
      if (res.data.pagination) {
        setPreviewTotal(res.data.pagination.total);
        setPreviewTotalPages(res.data.pagination.totalPages);
        setPreviewPage(res.data.pagination.page);
      } else {
        setPreviewTotal(res.data.total || 0);
        setPreviewTotalPages(Math.ceil((res.data.total || 0) / pPageSize));
      }
      
      setSelectedAsset(asset);
      setModalType('preview');
      setEditingRowId(null);
    } catch (err) {
      setPreviewError(err.response?.data?.detail || err.message || '加载预览失败');
    } finally {
      setPreviewLoading(false);
    }
  }, [previewPageSize, sortField, sortOrder]);

  const handlePreviewPageChange = (newPage) => {
    if (newPage < 1 || newPage > previewTotalPages) return;
    handlePreview(selectedAsset, newPage, previewPageSize, sortField, sortOrder);
  };

  const handlePreviewPageSizeChange = (newSize) => {
    setPreviewPageSize(newSize);
    handlePreview(selectedAsset, 1, newSize, sortField, sortOrder);
  };

  const handleSort = (field) => {
    const newOrder = sortField === field && sortOrder === 'asc' ? 'desc' : 'asc';
    setSortField(field);
    setSortOrder(newOrder);
    handlePreview(selectedAsset, 1, previewPageSize, field, newOrder);
  };

  const handleRetryPreview = () => {
    if (selectedAsset) {
      handlePreview(selectedAsset, previewPage, previewPageSize, sortField, sortOrder);
    }
  };

  const handleStructure = async (asset) => {
     try {
      const res = await getDataStructure(asset.path, asset.id);
      setStructureContent(res.data);
      setSelectedAsset(asset);
      setModalType('structure');
    } catch (err) {
      alert('加载结构失败: ' + err.message);
    }
  };
  
  const handleExportClick = async (asset) => {
      setSelectedAsset(asset);
      
      if (asset.source === 'minio') {
          try {
              const identifier = asset.id ? asset.id.toString() : asset.name;
              const res = await downloadDataAsset(identifier, 'minio');
              
              if (res.headers['content-type']?.includes('application/json')) {
                   const text = await res.data.text();
                   const json = JSON.parse(text);
                   if (json.status === 'minio_links') {
                       setModalType('minio_links');
                       setMinioLinks(json.links);
                       return;
                   }
              }
          } catch (err) {
              alert('获取下载链接失败: ' + err.message);
          }
      } else {
          setModalType('export');
      }
  };

  const handleExportConfirm = async () => {
      if (!selectedAsset) return;
      
      try {
          const identifier = selectedAsset.id ? selectedAsset.id.toString() : selectedAsset.name;
          const res = await downloadDataAsset(identifier, exportFormat);
          
          if (res.headers['content-type']?.includes('application/json')) {
              const text = await res.data.text();
              try {
                  const json = JSON.parse(text);
                  if (json.status === 'minio_links') {
                      setModalType('minio_links');
                      setMinioLinks(json.links);
                      return;
                  }
              } catch (e) {
              }
          }
          
          const url = window.URL.createObjectURL(new Blob([res.data]));
          const link = document.createElement('a');
          link.href = url;
          const contentDisposition = res.headers['content-disposition'];
          let filename = `${selectedAsset.name}.${exportFormat === 'excel' ? 'xlsx' : exportFormat}`;
          if (contentDisposition) {
              const match = contentDisposition.match(/filename=(.+)/);
              if (match) filename = match[1];
          }
          link.setAttribute('download', filename);
          document.body.appendChild(link);
          link.click();
          link.remove();
          setModalType(null);
      } catch (err) {
          alert('导出失败: ' + err.message);
      }
  };

  const closeModal = () => {
      setModalType(null);
      setPreviewContent(null);
      setMinioLinks(null);
      setStructureContent(null);
      setEditingRowId(null);
      setPreviewError(null);
      setSortField(null);
      setSortOrder(null);
  };

  const handleEditClick = (row) => {
      setEditingRowId(row._rowid);
      setEditData({...row});
  };

  const handleCancelEdit = () => {
      setEditingRowId(null);
      setEditData({});
  };

  const handleSaveClick = async () => {
      const editable = selectedAsset?.type === 'table' && selectedAsset?.source !== 'minio' && previewContent?.meta?.editable !== false;
      if (!editable) {
          alert("当前资产不支持编辑");
          return;
      }

      try {
          await updateTableRow(selectedAsset.path, editingRowId, editData);
          setEditingRowId(null);
          handlePreview(selectedAsset, previewPage, previewPageSize, sortField, sortOrder);
      } catch (err) {
          alert('更新失败: ' + (err.response?.data?.detail || err.message));
      }
  };

  const handleDeleteClick = async (rowId) => {
      const editable = selectedAsset?.type === 'table' && selectedAsset?.source !== 'minio' && previewContent?.meta?.editable !== false;
      if (!editable) {
          alert("当前资产不支持删除行");
          return;
      }

      if (confirm('确认删除此行?')) {
          try {
              await deleteTableRow(selectedAsset.path, rowId);
              handlePreview(selectedAsset, previewPage, previewPageSize, sortField, sortOrder);
          } catch (err) {
              alert('删除失败: ' + (err.response?.data?.detail || err.message));
          }
      }
  };

  const handleInputChange = (col, value) => {
      setEditData(prev => ({ ...prev, [col]: value }));
  };

  const totalPages = Math.ceil(total / pageSize);

  const [jumpPage, setJumpPage] = useState('');
  const [previewJumpPage, setPreviewJumpPage] = useState('');

  const handleJump = () => {
      const p = parseInt(jumpPage);
      if (!isNaN(p) && p >= 1 && p <= totalPages) {
          setPage(p);
          setJumpPage('');
      }
  };

  const handlePreviewJump = () => {
      const p = parseInt(previewJumpPage);
      if (!isNaN(p) && p >= 1 && p <= previewTotalPages) {
          handlePreviewPageChange(p);
          setPreviewJumpPage('');
      }
  };

  const renderSortIcon = (field) => {
    if (sortField !== field) return null;
    return sortOrder === 'asc' ? <ArrowUp size={12} className="inline ml-1" /> : <ArrowDown size={12} className="inline ml-1" />;
  };

  return (
    <div className="space-y-6 flex flex-col h-[calc(100vh-8rem)]">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-slate-800 flex items-center gap-2">
          <Folder className="text-purple-500" /> 数据管理
        </h2>
      </div>

      <div className="bg-white border border-slate-200 rounded-lg shadow-sm overflow-hidden">
        <div className="p-4 flex gap-4 items-center">
            <div className="relative flex-1 max-w-xs">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={16} />
                <input 
                    type="text" 
                    placeholder="按名称搜索..." 
                    className="w-full bg-slate-50 border border-slate-200 rounded-md pl-9 pr-4 py-2 text-sm text-slate-700 focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/20"
                    value={filters.name}
                    onChange={e => handleFilterChange('name', e.target.value)}
                    onKeyDown={e => e.key === 'Enter' && fetchAssets()}
                />
            </div>
            
            <div className="w-48">
                <select 
                    className="w-full bg-slate-50 border border-slate-200 rounded-md px-3 py-2 text-sm text-slate-700 focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/20"
                    value={filters.type}
                    onChange={e => handleFilterChange('type', e.target.value)}
                >
                    <option value="">所有类型</option>
                    {ASSET_TYPE_OPTIONS.map(opt => (
                      <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))}
                </select>
            </div>

            <button
              onClick={() => setIsFilterExpanded(!isFilterExpanded)}
              className="flex items-center gap-1 text-sm text-slate-600 hover:text-slate-800 px-3 py-2 rounded-md hover:bg-slate-50"
            >
              {isFilterExpanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
              更多筛选
              {activeFilterCount > 0 && (
                <span className="bg-purple-100 text-purple-600 text-xs px-1.5 py-0.5 rounded-full ml-1">
                  {activeFilterCount}
                </span>
              )}
            </button>

            {activeFilterCount > 0 && (
              <button
                onClick={clearFilters}
                className="text-sm text-slate-500 hover:text-slate-700 px-2 py-1"
              >
                清除筛选
              </button>
            )}
        </div>

        {isFilterExpanded && (
          <div className="px-4 pb-4 pt-0 border-t border-slate-100 flex gap-4 items-center">
            <div className="flex items-center gap-2">
              <label className="text-sm text-slate-500">数据类型:</label>
              <select 
                  className="bg-slate-50 border border-slate-200 rounded-md px-3 py-1.5 text-sm text-slate-700 focus:outline-none focus:border-purple-500"
                  value={filters.data_type}
                  onChange={e => handleFilterChange('data_type', e.target.value)}
              >
                  <option value="">全部</option>
                  {DATA_TYPE_OPTIONS.map(opt => (
                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                  ))}
              </select>
            </div>
            
            <div className="text-sm text-slate-400 ml-auto">
              找到 <span className="font-medium text-slate-600">{total}</span> 条结果
              {activeFilterCount > 0 && (
                <span className="ml-2 text-purple-500">({activeFilterCount} 个筛选条件)</span>
              )}
            </div>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-auto">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {filteredAssets.map((asset, idx) => (
              <div key={idx} className="bg-white border border-slate-200 rounded-lg overflow-hidden hover:border-purple-500 hover:shadow-md transition-all group relative">
                  <div className="p-5">
                      <div className="flex items-start justify-between mb-4">
                          <div className={`p-3 rounded-lg ${
                              asset.type === 'table' ? 'bg-purple-50 text-purple-600' : 
                              asset.type === 'bucket' ? 'bg-orange-50 text-orange-600' :
                              'bg-slate-100 text-slate-500'
                          }`}>
                              {asset.type === 'table' ? <Database size={24} /> : 
                               asset.type === 'bucket' ? <Folder size={24} /> :
                               <FileText size={24} />}
                          </div>
                          <div className="flex gap-2 items-center flex-wrap">
                              <span className={`text-xs font-mono border px-2 py-0.5 rounded ${
                                  asset.type === 'table' ? 'border-purple-200 text-purple-600 bg-purple-50' : 
                                  asset.type === 'bucket' ? 'border-orange-200 text-orange-600 bg-orange-50' :
                                  'border-slate-200 text-slate-500 bg-slate-50'
                              }`}>
                                  {asset.source || asset.type}
                              </span>
                              {asset.data_type && (
                                <span className={`text-xs font-medium px-2 py-0.5 rounded border ${
                                  asset.data_type === 'IMAGE' ? 'bg-blue-50 text-blue-600 border-blue-200' :
                                  asset.data_type === 'TIMESERIES' ? 'bg-green-50 text-green-600 border-green-200' :
                                  asset.data_type === 'TEXT' ? 'bg-purple-50 text-purple-600 border-purple-200' :
                                  'bg-slate-100 text-slate-500 border-slate-200'
                                }`}>
                                  {DATA_TYPE_OPTIONS.find(o => o.value === asset.data_type)?.label || asset.data_type}
                                </span>
                              )}
                              <button 
                                  onClick={(e) => { e.stopPropagation(); handleDeleteAsset(asset); }}
                                  className="p-1.5 rounded-full bg-slate-50 text-slate-400 hover:text-rose-500 hover:bg-rose-50 transition-all z-10"
                                  title="删除资产"
                              >
                                  <Trash2 size={14} />
                              </button>
                          </div>
                      </div>
                      
                      <h3 className="text-lg font-semibold text-slate-800 mb-1 truncate" title={asset.name}>
                          {asset.name}
                      </h3>
                      <p className="text-sm text-slate-500 mb-4 line-clamp-2 h-10">
                          从 {asset.source || '未知数据源'} 导入
                      </p>

                      <div className="flex items-center gap-4 text-xs text-slate-400 font-mono mb-4">
                          <span>{asset.size}</span>
                          {asset.rows > 0 && <span>{asset.rows} 行</span>}
                      </div>

                      <div className="flex gap-2 pt-4 border-t border-slate-100">
                          <button 
                            onClick={() => handlePreview(asset)}
                            className="flex-1 flex items-center justify-center gap-2 py-2 rounded bg-slate-50 hover:bg-slate-100 text-slate-600 text-sm transition-colors"
                          >
                              <Eye size={16} /> 预览
                          </button>
                          <button 
                            onClick={() => handleStructure(asset)}
                            className="p-2 rounded bg-slate-50 hover:bg-slate-100 text-slate-600 transition-colors"
                            title="结构"
                          >
                              <TableIcon size={16} />
                          </button>
                          <button 
                            onClick={() => handleExportClick(asset)}
                            className="p-2 rounded bg-slate-50 hover:bg-slate-100 text-slate-600 transition-colors"
                            title="导出"
                          >
                              <Download size={16} />
                          </button>
                      </div>
                  </div>
              </div>
          ))}
          {filteredAssets.length === 0 && (
            <div className="col-span-full p-12 border border-dashed border-slate-300 rounded-lg text-center text-slate-400">
                <Folder size={48} className="mx-auto mb-4 opacity-50" />
                <p>未找到匹配的数据资产。</p>
                {activeFilterCount > 0 && (
                  <button onClick={clearFilters} className="mt-2 text-purple-500 hover:text-purple-600 text-sm">
                    清除筛选条件
                  </button>
                )}
            </div>
          )}
      </div>
      </div>
      
      <div className="mt-auto pt-4 border-t border-slate-200 flex items-center justify-between">
          <div className="flex items-center gap-4">
              <span className="text-sm text-slate-500">
                  共 {total} 条
              </span>
              <div className="flex items-center gap-2">
                  <label className="text-sm text-slate-500">每页:</label>
                  <select 
                      value={pageSize}
                      onChange={e => { setPageSize(parseInt(e.target.value)); setPage(1); }}
                      className="bg-white border border-slate-200 rounded px-2 py-1 text-sm text-slate-700 focus:outline-none focus:border-purple-500"
                  >
                      {PAGE_SIZE_OPTIONS.map(opt => (
                          <option key={opt} value={opt}>{opt}</option>
                      ))}
                  </select>
              </div>
          </div>
          <div className="flex gap-2 items-center">
             <div className="flex items-center gap-2 mr-4">
                  <span className="text-sm text-slate-500">前往</span>
                  <input 
                      type="number" 
                      min="1" 
                      max={totalPages || 1}
                      className="w-12 h-8 text-center bg-white border border-slate-200 rounded text-sm text-slate-700 focus:outline-none focus:border-purple-500"
                      value={jumpPage}
                      onChange={e => setJumpPage(e.target.value)}
                      onKeyDown={e => e.key === 'Enter' && handleJump()}
                  />
                  <span className="text-sm text-slate-500">页</span>
              </div>
              <button 
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="w-8 h-8 flex items-center justify-center rounded bg-white border border-slate-200 text-slate-600 hover:text-slate-800 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
              >
                  <ChevronLeft size={16} />
              </button>
              <div className="flex items-center gap-1">
                  {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
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
                                  ? 'bg-purple-600 text-white shadow-sm' 
                                  : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-50'
                              }`}
                          >
                              {p}
                          </button>
                      );
                  })}
              </div>
              <button 
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages}
                  className="w-8 h-8 flex items-center justify-center rounded bg-white border border-slate-200 text-slate-600 hover:text-slate-800 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
              >
                  <ChevronRight size={16} />
              </button>
          </div>
      </div>

      <Modal isOpen={modalType === 'preview'} onClose={closeModal} title={`预览: ${selectedAsset?.name}`}>
        <div className="flex flex-col h-[70vh]">
            {previewLoading && (
                <div className="flex items-center justify-center h-32">
                    <Loader2 size={32} className="animate-spin text-purple-500" />
                    <span className="ml-2 text-slate-500">加载中...</span>
                </div>
            )}
            
            {previewError && (
                <div className="flex flex-col items-center justify-center h-32 text-rose-500">
                    <AlertTriangle size={32} />
                    <span className="mt-2">{previewError}</span>
                    <button 
                        onClick={handleRetryPreview}
                        className="mt-2 px-4 py-1 bg-rose-100 text-rose-600 rounded hover:bg-rose-200 text-sm"
                    >
                        重试
                    </button>
                </div>
            )}
            
            {!previewLoading && !previewError && previewContent && (
                <>
                    <div className="flex items-center justify-between mb-4 px-2">
                        <div className="flex items-center gap-4">
                            <span className="text-sm text-slate-400">
                                显示 {((previewPage - 1) * previewPageSize) + 1} - {Math.min(previewPage * previewPageSize, previewTotal)} 共 {previewTotal} 行
                            </span>
                            <div className="flex items-center gap-2">
                                <label className="text-sm text-slate-500">每页:</label>
                                <select 
                                    value={previewPageSize}
                                    onChange={e => handlePreviewPageSizeChange(parseInt(e.target.value))}
                                    className="bg-slate-800 border border-slate-700 rounded px-2 py-1 text-sm text-slate-300 focus:outline-none focus:border-purple-500"
                                >
                                    {PREVIEW_PAGE_SIZE_OPTIONS.map(opt => (
                                        <option key={opt} value={opt}>{opt}</option>
                                    ))}
                                </select>
                            </div>
                        </div>
                        {previewContent.sort && (
                            <span className="text-xs text-slate-500">
                                排序: {previewContent.sort.field} ({previewContent.sort.order})
                            </span>
                        )}
                    </div>
                    
                    <div className="overflow-auto flex-1 border border-slate-700 rounded">
                        <table className="w-full text-left text-xs border-collapse">
                            <thead className="sticky top-0 z-10">
                                <tr className="bg-slate-800 text-slate-300 shadow-sm">
                                    {selectedAsset?.type === 'table' && selectedAsset?.source !== 'minio' && previewContent?.meta?.editable !== false && (
                                        <th className="p-2 border border-slate-700 w-24 bg-slate-800">操作</th>
                                    )}
                                    {previewContent.columns.filter(c => c !== '_rowid').map(col => (
                                        <th 
                                            key={col} 
                                            className="p-2 border border-slate-700 bg-slate-800 whitespace-nowrap cursor-pointer hover:bg-slate-700"
                                            onClick={() => handleSort(col)}
                                        >
                                            {col} {renderSortIcon(col)}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {previewContent.data.length === 0 ? (
                                    <tr>
                                        <td colSpan={previewContent.columns.length + 1} className="p-8 text-center text-slate-500">
                                            无数据
                                        </td>
                                    </tr>
                                ) : (
                                    previewContent.data.map((row, i) => (
                                        <tr key={i} className="hover:bg-slate-800/50 group">
                                            {selectedAsset?.type === 'table' && selectedAsset?.source !== 'minio' && previewContent?.meta?.editable !== false && (
                                                <td className="p-2 border border-slate-700 whitespace-nowrap">
                                                    {editingRowId === row._rowid ? (
                                                        <div className="flex gap-2">
                                                            <button onClick={handleSaveClick} className="text-emerald-500 hover:text-emerald-400"><Check size={14}/></button>
                                                            <button onClick={handleCancelEdit} className="text-rose-500 hover:text-rose-400"><X size={14}/></button>
                                                        </div>
                                                    ) : (
                                                        <div className="flex gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                                                            <button onClick={() => handleEditClick(row)} className="text-blue-500 hover:text-blue-400"><Edit2 size={14}/></button>
                                                            <button onClick={() => handleDeleteClick(row._rowid)} className="text-rose-500 hover:text-rose-400"><Trash2 size={14}/></button>
                                                        </div>
                                                    )}
                                                </td>
                                            )}
                                            {previewContent.columns.filter(c => c !== '_rowid').map(col => (
                                                <td key={col} className="p-2 border border-slate-700 text-slate-400 whitespace-nowrap max-w-[200px] truncate">
                                                    {editingRowId === row._rowid ? (
                                                        <input 
                                                            className="w-full bg-slate-900 border border-slate-600 rounded px-1 text-slate-200 focus:border-purple-500 outline-none"
                                                            value={editData[col] !== null ? editData[col] : ''}
                                                            onChange={e => handleInputChange(col, e.target.value)}
                                                        />
                                                    ) : (
                                                        <span title={String(row[col])}>{String(row[col])}</span>
                                                    )}
                                                </td>
                                            ))}
                                        </tr>
                                    ))
                                )}
                            </tbody>
                        </table>
                    </div>
                    
                    <div className="flex justify-between items-center text-sm text-slate-400 border-t border-slate-700 pt-4 mt-4">
                        <div className="flex items-center gap-2">
                            <button 
                                onClick={() => handlePreviewPageChange(1)}
                                disabled={previewPage <= 1}
                                className="px-2 py-1 rounded hover:bg-slate-800 disabled:opacity-50 disabled:hover:bg-transparent text-xs"
                            >
                                首页
                            </button>
                            <button 
                                onClick={() => handlePreviewPageChange(previewPage - 1)}
                                disabled={previewPage <= 1}
                                className="p-1 rounded hover:bg-slate-800 disabled:opacity-50 disabled:hover:bg-transparent"
                            >
                                <ChevronLeft size={20} />
                            </button>
                        </div>
                        
                        <div className="flex items-center gap-2">
                            <input 
                                type="number" 
                                min="1" 
                                max={previewTotalPages || 1}
                                className="w-12 h-7 text-center bg-slate-800 border border-slate-700 rounded text-sm text-slate-300 focus:outline-none focus:border-purple-500"
                                value={previewJumpPage}
                                onChange={e => setPreviewJumpPage(e.target.value)}
                                onKeyDown={e => e.key === 'Enter' && handlePreviewJump()}
                            />
                            <span className="font-mono">/ {previewTotalPages || 1}</span>
                        </div>
                        
                        <div className="flex items-center gap-2">
                            <button 
                                onClick={() => handlePreviewPageChange(previewPage + 1)}
                                disabled={previewPage >= previewTotalPages}
                                className="p-1 rounded hover:bg-slate-800 disabled:opacity-50 disabled:hover:bg-transparent"
                            >
                                <ChevronRight size={20} />
                            </button>
                            <button 
                                onClick={() => handlePreviewPageChange(previewTotalPages)}
                                disabled={previewPage >= previewTotalPages}
                                className="px-2 py-1 rounded hover:bg-slate-800 disabled:opacity-50 disabled:hover:bg-transparent text-xs"
                            >
                                末页
                            </button>
                        </div>
                    </div>
                </>
            )}
        </div>
      </Modal>

      <Modal isOpen={modalType === 'structure'} onClose={closeModal} title={`结构: ${selectedAsset?.name}`}>
        <div className="overflow-auto max-h-[60vh]">
            {structureContent ? (
                <table className="w-full text-left text-sm border-collapse">
                    <thead>
                        <tr className="bg-slate-800 text-slate-300">
                            <th className="p-2 border border-slate-700">列名</th>
                            <th className="p-2 border border-slate-700">类型</th>
                            <th className="p-2 border border-slate-700">可空</th>
                        </tr>
                    </thead>
                    <tbody>
                        {structureContent.map((col, i) => (
                            <tr key={i} className="hover:bg-slate-800/50">
                                <td className="p-2 border border-slate-700 text-slate-200">{col.name}</td>
                                <td className="p-2 border border-slate-700 text-slate-400 font-mono">{col.type}</td>
                                <td className="p-2 border border-slate-700 text-slate-400">{col.nullable ? '是' : '否'}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            ) : (
                <div className="p-8 text-center text-slate-500">加载中...</div>
            )}
        </div>
      </Modal>
      
      <Modal isOpen={modalType === 'export'} onClose={closeModal} title={`导出: ${selectedAsset?.name}`}>
          <div className="p-4 space-y-4">
              <p className="text-sm text-slate-600">请选择导出格式:</p>
              <div className="flex gap-4">
                  <label className="flex items-center gap-2 cursor-pointer">
                      <input 
                        type="radio" 
                        name="format" 
                        value="csv" 
                        checked={exportFormat === 'csv'} 
                        onChange={e => setExportFormat(e.target.value)}
                        className="text-purple-600 focus:ring-purple-500"
                      />
                      <span className="text-sm text-slate-700">CSV</span>
                  </label>
                  <label className="flex items-center gap-2 cursor-pointer">
                      <input 
                        type="radio" 
                        name="format" 
                        value="excel" 
                        checked={exportFormat === 'excel'} 
                        onChange={e => setExportFormat(e.target.value)}
                        className="text-purple-600 focus:ring-purple-500"
                      />
                      <span className="text-sm text-slate-700">Excel (XLSX)</span>
                  </label>
                  <label className="flex items-center gap-2 cursor-pointer">
                      <input 
                        type="radio" 
                        name="format" 
                        value="json" 
                        checked={exportFormat === 'json'} 
                        onChange={e => setExportFormat(e.target.value)}
                        className="text-purple-600 focus:ring-purple-500"
                      />
                      <span className="text-sm text-slate-700">JSON</span>
                  </label>
              </div>
              <div className="flex justify-end pt-4">
                  <button 
                      onClick={handleExportConfirm}
                      className="px-4 py-2 bg-purple-600 text-white rounded hover:bg-purple-500 transition-colors text-sm"
                  >
                      确认导出
                  </button>
              </div>
          </div>
      </Modal>

      <Modal isOpen={modalType === 'minio_links'} onClose={closeModal} title={`下载文件: ${selectedAsset?.name}`}>
           <div className="p-4 overflow-auto max-h-[60vh]">
               <p className="text-sm text-slate-500 mb-4">以下是 Bucket 中的文件下载链接 (有效期5分钟):</p>
               <ul className="space-y-2">
                   {minioLinks && Array.isArray(minioLinks) ? minioLinks.map((item, idx) => (
                       <li key={idx} className="flex items-center justify-between p-2 bg-slate-50 rounded border border-slate-100">
                           <span className="text-sm font-mono text-slate-700 truncate max-w-[300px]" title={item.key}>{item.key}</span>
                           <a 
                             href={item.url} 
                             target="_blank" 
                             rel="noreferrer"
                             className="text-xs text-blue-600 hover:text-blue-500 hover:underline flex items-center gap-1"
                           >
                               <Download size={12} /> 下载
                           </a>
                       </li>
                   )) : <p className="text-sm text-slate-400">无文件或加载失败。</p>}
               </ul>
           </div>
      </Modal>

    </div>
  );
};

export default DataManagementPage;