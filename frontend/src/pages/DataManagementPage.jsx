import React, { useState, useEffect } from 'react';
import { Folder, Eye, FileText, Download, Table as TableIcon, Database, Trash2, Edit2, Check, X, ChevronLeft, ChevronRight } from 'lucide-react';
import { getDataAssets, deleteDataAsset, previewData, getDataStructure, updateTableRow, deleteTableRow, downloadDataAsset } from '../api';
import { Modal } from '../components/Common';
import { Search, Filter, MoreVertical } from 'lucide-react';

const DataManagementPage = () => {
  const [assets, setAssets] = useState([]);
  const [filteredAssets, setFilteredAssets] = useState([]);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [previewContent, setPreviewContent] = useState(null);
  const [minioLinks, setMinioLinks] = useState(null);
  const [structureContent, setStructureContent] = useState(null);
  const [modalType, setModalType] = useState(null); // 'preview' or 'structure' or 'export'
  const [exportFormat, setExportFormat] = useState('csv');
  
  // Filters
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('all'); // 'all', 'file', 'table'

  // Pagination & Editing State
  const [page, setPage] = useState(1);
  const [pageSize] = useState(12); // Data cards per page
  const [total, setTotal] = useState(0);
  const [previewPage, setPreviewPage] = useState(1);
  const [previewPageSize] = useState(20);
  const [previewTotal, setPreviewTotal] = useState(0);

  const [editingRowId, setEditingRowId] = useState(null);
  const [editData, setEditData] = useState({});
  const [selectedAssetIds, setSelectedAssetIds] = useState([]); // For bulk delete

  useEffect(() => {
    fetchAssets();
  }, []);

  useEffect(() => {
      let result = assets;
      
      // Filter by name
      if (searchTerm) {
          result = result.filter(a => a.name.toLowerCase().includes(searchTerm.toLowerCase()));
      }
      
      // Filter by type
      if (filterType !== 'all') {
          result = result.filter(a => a.type === filterType);
      }
      
      setTotal(result.length);
      // Pagination logic for assets
      const start = (page - 1) * pageSize;
      const end = start + pageSize;
      setFilteredAssets(result.slice(start, end));
  }, [assets, searchTerm, filterType, page, pageSize]);

  const fetchAssets = async () => {
    try {
      const res = await getDataAssets();
      setAssets(res.data);
      setTotal(res.data.length); // Initialize total
    } catch (err) {
      console.error(err);
    }
  };

  const handleDeleteAsset = async (asset) => {
      // 1. Double check before deletion
      if (confirm(`Are you sure you want to delete ${asset.name}? This action cannot be undone.`)) {
          try {
              // Use ID if available, otherwise name (backward compat)
              const identifier = asset.id ? asset.id.toString() : asset.name;
              await deleteDataAsset(identifier);
              fetchAssets(); // Refresh list
          } catch (err) {
              alert('Failed to delete asset: ' + (err.response?.data?.detail || err.message));
          }
      }
  };

  const handlePreview = async (asset, pageNum = 1) => {
    try {
      const offset = (pageNum - 1) * previewPageSize;
      // Pass ID as query param or part of path? 
      // API: previewData(path, limit, offset, id)
      // We need to update api.js to support id parameter
      // For now, let's assume previewData accepts optional 4th param or object
      // Actually, looking at api.js is needed. I will update it.
      // But assuming I can pass it.
      
      const res = await previewData(asset.path, previewPageSize, offset, asset.id);
      setPreviewContent(res.data);
      setPreviewTotal(res.data.total || 0); 
      setSelectedAsset(asset);
      setModalType('preview');
      setPreviewPage(pageNum);
      setEditingRowId(null);
    } catch (err) {
      alert('Failed to load preview: ' + err.message);
    }
  };

  const handlePreviewPageChange = (newPage) => {
      if (newPage < 1) return;
      handlePreview(selectedAsset, newPage);
  };

  const handleStructure = async (asset) => {
     try {
      const res = await getDataStructure(asset.path, asset.id);
      setStructureContent(res.data);
      setSelectedAsset(asset);
      setModalType('structure');
    } catch (err) {
      alert('Failed to load structure: ' + err.message);
    }
  };
  
  const handleExportClick = async (asset) => {
      setSelectedAsset(asset);
      
      // If it's a MinIO asset (based on type/source inference or if we stored source_type in asset list),
      // we should skip the format selection. 
      // Current 'getDataAssets' returns { name, type, size, source, path }.
      // 'source' is the source_type (mysql, clickhouse, minio).
      
      if (asset.source === 'minio') {
          // Trigger export directly for MinIO to get links
          // We can reuse handleExportConfirm logic but need to set state properly
          // Or just call API here.
          try {
              const identifier = asset.id ? asset.id.toString() : asset.name;
              const res = await downloadDataAsset(identifier, 'minio'); // format ignored for minio
              
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
              alert('Failed to get download links: ' + err.message);
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
          
          // Check if it's MinIO links (JSON) or Blob
          if (res.headers['content-type']?.includes('application/json')) {
              // It's a JSON response with links? 
              // Wait, axios response.data is Blob if responseType is blob.
              // If backend returns JSON, blob will contain text.
              const text = await res.data.text();
              try {
                  const json = JSON.parse(text);
                  if (json.status === 'minio_links') {
                      // Show links in a new modal or alert
                      setModalType('minio_links');
                      setMinioLinks(json.links); // Reuse previewContent for links
                      return;
                  }
              } catch (e) {
                  // Not JSON, proceed as file
              }
          }
          
          // It's a file download
          const url = window.URL.createObjectURL(new Blob([res.data]));
          const link = document.createElement('a');
          link.href = url;
          // Try to get filename from header
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
          alert('Export failed: ' + err.message);
      }
  };

  const closeModal = () => {
      setModalType(null);
      setPreviewContent(null);
      setMinioLinks(null);
      setStructureContent(null);
      setEditingRowId(null);
  };

  // Editing Handlers
  const handleEditClick = (row) => {
      setEditingRowId(row._rowid);
      setEditData({...row});
  };

  const handleCancelEdit = () => {
      setEditingRowId(null);
      setEditData({});
  };

  const handleSaveClick = async () => {
      // If MinIO, editing is not supported yet (it's object storage metadata)
      if (selectedAsset.source === 'minio') {
          alert("MinIO 资产暂不支持编辑元数据");
          return;
      }

      try {
          await updateTableRow(selectedAsset.path, editingRowId, editData);
          setEditingRowId(null);
          handlePreview(selectedAsset, previewPage); // Refresh data
      } catch (err) {
          alert('Failed to update row: ' + (err.response?.data?.detail || err.message));
      }
  };

  const handleDeleteClick = async (rowId) => {
      // If MinIO, deleting objects is possible but maybe restricted in this view?
      // Let's block for now to be safe or implement later.
      if (selectedAsset.source === 'minio') {
          // rowId is ETag, we need Key to delete.
          // The preview data has 'Key'.
          // But rowId might not be enough if we don't look up the row.
          alert("MinIO 资产暂不支持在此处删除对象");
          return;
      }

      if (confirm('Are you sure you want to delete this row?')) {
          try {
              await deleteTableRow(selectedAsset.path, rowId);
              handlePreview(selectedAsset, previewPage); // Refresh data
          } catch (err) {
              alert('Failed to delete row: ' + (err.response?.data?.detail || err.message));
          }
      }
  };

  const handleInputChange = (col, value) => {
      setEditData(prev => ({ ...prev, [col]: value }));
  };

  const totalPages = Math.ceil(total / pageSize);
  const totalPreviewPages = Math.ceil(previewTotal / previewPageSize);

  const [jumpPage, setJumpPage] = useState('');

  const handleJump = () => {
      const p = parseInt(jumpPage);
      if (!isNaN(p) && p >= 1 && p <= totalPages) {
          setPage(p);
          setJumpPage('');
      }
  };

  return (
    <div className="space-y-6 flex flex-col h-[calc(100vh-8rem)]">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-slate-800 flex items-center gap-2">
          <Folder className="text-purple-500" /> 数据管理
        </h2>
      </div>

      <div className="bg-white border border-slate-200 rounded-lg p-4 flex gap-4 items-center shadow-sm">
          <div className="relative flex-1 max-w-xs">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={16} />
              <input 
                  type="text" 
                  placeholder="按名称搜索..." 
                  className="w-full bg-slate-50 border border-slate-200 rounded-md pl-9 pr-4 py-2 text-sm text-slate-700 focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/20"
                  value={searchTerm}
                  onChange={e => setSearchTerm(e.target.value)}
              />
          </div>
          
          <div className="relative w-48">
              <Filter className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={16} />
              <select 
                  className="w-full bg-slate-50 border border-slate-200 rounded-md pl-9 pr-4 py-2 text-sm text-slate-700 focus:outline-none focus:border-purple-500 focus:ring-1 focus:ring-purple-500/20 appearance-none cursor-pointer"
                  value={filterType}
                  onChange={e => setFilterType(e.target.value)}
              >
                  <option value="all">所有类型</option>
                  <option value="table">数据库表</option>
                  <option value="bucket">MinIO 存储桶</option>
                  <option value="file">本地文件</option>
              </select>
          </div>
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
                          <div className="flex gap-2 items-center">
                              <span className={`text-xs font-mono border px-2 py-0.5 rounded ${
                                  asset.type === 'table' ? 'border-purple-200 text-purple-600 bg-purple-50' : 
                                  asset.type === 'bucket' ? 'border-orange-200 text-orange-600 bg-orange-50' :
                                  'border-slate-200 text-slate-500 bg-slate-50'
                              }`}>
                                  {asset.source || asset.type}
                              </span>
                              {/* Delete Button (always visible) */}
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
            </div>
          )}
      </div>
      </div>
      
      {/* Asset Pagination */}
      <div className="mt-auto pt-4 border-t border-slate-200 flex items-center justify-between">
          <div className="text-sm text-slate-500">
             共 {total} 条
          </div>
          <div className="flex gap-2 items-center">
             <div className="flex items-center gap-2 mr-4">
                  <span className="text-sm text-slate-500">前往</span>
                  <input 
                      type="number" 
                      min="1" 
                      max={totalPages}
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

      {/* Preview Modal */}
      <Modal isOpen={modalType === 'preview'} onClose={closeModal} title={`预览: ${selectedAsset?.name}`}>
        <div className="flex flex-col h-[70vh]">
            <div className="overflow-auto flex-1 border border-slate-700 rounded mb-4">
                {previewContent ? (
                    <table className="w-full text-left text-xs border-collapse">
                        <thead className="sticky top-0 z-10">
                            <tr className="bg-slate-800 text-slate-300 shadow-sm">
                                {selectedAsset?.type === 'table' && selectedAsset?.source !== 'minio' && <th className="p-2 border border-slate-700 w-24 bg-slate-800">操作</th>}
                                {previewContent.columns.filter(c => c !== '_rowid').map(col => (
                                    <th key={col} className="p-2 border border-slate-700 bg-slate-800 whitespace-nowrap">{col}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {previewContent.data.map((row, i) => (
                                <tr key={i} className="hover:bg-slate-800/50 group">
                                    {selectedAsset?.type === 'table' && selectedAsset?.source !== 'minio' && (
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
                            ))}
                        </tbody>
                    </table>
                ) : (
                    <div className="p-8 text-center text-slate-500 flex items-center justify-center h-full">加载中...</div>
                )}
            </div>
            
            {/* Preview Pagination */}
            {previewContent && (
                <div className="flex justify-between items-center text-sm text-slate-400 border-t border-slate-700 pt-4">
                    <div>
                        显示 {((previewPage - 1) * previewPageSize) + 1} - {Math.min(previewPage * previewPageSize, previewTotal)} 共 {previewTotal} 行
                    </div>
                    <div className="flex items-center gap-2">
                        <button 
                            onClick={() => handlePreviewPageChange(previewPage - 1)}
                            disabled={previewPage <= 1}
                            className="p-1 rounded hover:bg-slate-800 disabled:opacity-50 disabled:hover:bg-transparent"
                        >
                            <ChevronLeft size={20} />
                        </button>
                        <span className="font-mono bg-slate-800 px-2 py-1 rounded">{previewPage} / {totalPreviewPages || 1}</span>
                        <button 
                            onClick={() => handlePreviewPageChange(previewPage + 1)}
                            disabled={previewPage >= totalPreviewPages}
                            className="p-1 rounded hover:bg-slate-800 disabled:opacity-50 disabled:hover:bg-transparent"
                        >
                            <ChevronRight size={20} />
                        </button>
                    </div>
                </div>
            )}
        </div>
      </Modal>

      {/* Structure Modal - Unchanged mostly */}
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
      
      {/* Export Modal */}
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

      {/* MinIO Links Modal */}
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