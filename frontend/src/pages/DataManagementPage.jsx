import React, { useState, useEffect } from 'react';
import { Folder, Eye, FileText, Download, Table as TableIcon, Database, Trash2, Edit2, Check, X, ChevronLeft, ChevronRight } from 'lucide-react';
import { getDataAssets, previewData, getDataStructure, updateTableRow, deleteTableRow } from '../api';
import { Modal } from '../components/Common';

const DataManagementPage = () => {
  const [assets, setAssets] = useState([]);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [previewContent, setPreviewContent] = useState(null);
  const [structureContent, setStructureContent] = useState(null);
  const [modalType, setModalType] = useState(null); // 'preview' or 'structure'
  
  // Pagination & Editing State
  const [page, setPage] = useState(1);
  const [pageSize] = useState(10);
  const [total, setTotal] = useState(0);
  const [editingRowId, setEditingRowId] = useState(null);
  const [editData, setEditData] = useState({});

  useEffect(() => {
    fetchAssets();
  }, []);

  const fetchAssets = async () => {
    try {
      const res = await getDataAssets();
      setAssets(res.data);
    } catch (err) {
      console.error(err);
    }
  };

  const handlePreview = async (asset, pageNum = 1) => {
    try {
      const offset = (pageNum - 1) * pageSize;
      const res = await previewData(asset.path, pageSize, offset);
      setPreviewContent(res.data);
      setTotal(res.data.total || 0); // Backend now returns total
      setSelectedAsset(asset);
      setModalType('preview');
      setPage(pageNum);
      setEditingRowId(null);
    } catch (err) {
      alert('Failed to load preview: ' + err.message);
    }
  };

  const handlePageChange = (newPage) => {
      if (newPage < 1) return;
      handlePreview(selectedAsset, newPage);
  };

  const handleStructure = async (asset) => {
     try {
      const res = await getDataStructure(asset.path);
      setStructureContent(res.data);
      setSelectedAsset(asset);
      setModalType('structure');
    } catch (err) {
      alert('Failed to load structure: ' + err.message);
    }
  };
  
  const handleExport = (asset) => {
      alert(`Download link generated for ${asset.name}: /api/download/${asset.name}`);
  };

  const closeModal = () => {
      setModalType(null);
      setPreviewContent(null);
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
      try {
          await updateTableRow(selectedAsset.path, editingRowId, editData);
          setEditingRowId(null);
          handlePreview(selectedAsset, page); // Refresh data
      } catch (err) {
          alert('Failed to update row: ' + (err.response?.data?.detail || err.message));
      }
  };

  const handleDeleteClick = async (rowId) => {
      if (confirm('Are you sure you want to delete this row?')) {
          try {
              await deleteTableRow(selectedAsset.path, rowId);
              handlePreview(selectedAsset, page); // Refresh data
          } catch (err) {
              alert('Failed to delete row: ' + (err.response?.data?.detail || err.message));
          }
      }
  };

  const handleInputChange = (col, value) => {
      setEditData(prev => ({ ...prev, [col]: value }));
  };

  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
          <Folder className="text-purple-500" /> 数据管理
        </h2>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {assets.map((asset, idx) => (
              <div key={idx} className="bg-slate-800/50 border border-slate-700 rounded-lg overflow-hidden hover:border-purple-500/50 transition-all group">
                  <div className="p-5">
                      <div className="flex items-start justify-between mb-4">
                          <div className={`p-3 rounded-lg ${asset.type === 'table' ? 'bg-purple-900/20 text-purple-400' : 'bg-slate-900 text-slate-400'}`}>
                              {asset.type === 'table' ? <Database size={24} /> : <FileText size={24} />}
                          </div>
                          <span className={`text-xs font-mono border px-2 py-0.5 rounded ${asset.type === 'table' ? 'border-purple-500/30 text-purple-400' : 'border-slate-700 text-slate-500'}`}>
                              {asset.type === 'table' ? '表' : '文件'}
                          </span>
                      </div>
                      
                      <h3 className="text-lg font-semibold text-slate-200 mb-1 truncate" title={asset.name}>
                          {asset.name}
                      </h3>
                      <p className="text-sm text-slate-400 mb-4 line-clamp-2 h-10">
                          {asset.type === 'table' ? '已同步的数据库表' : `位于 ${asset.path} 的导入数据资产`}
                      </p>

                      <div className="flex items-center gap-4 text-xs text-slate-500 font-mono mb-4">
                          <span>{asset.size}</span>
                          <span>•</span>
                          <span>{asset.rows || '?'} 行</span>
                      </div>

                      <div className="flex gap-2 pt-4 border-t border-slate-700/50">
                          <button 
                            onClick={() => handlePreview(asset)}
                            className="flex-1 flex items-center justify-center gap-2 py-2 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm transition-colors"
                          >
                              <Eye size={16} /> 预览
                          </button>
                          <button 
                            onClick={() => handleStructure(asset)}
                            className="p-2 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 transition-colors"
                            title="结构"
                          >
                              <TableIcon size={16} />
                          </button>
                          <button 
                            onClick={() => handleExport(asset)}
                            className="p-2 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 transition-colors"
                            title="导出"
                          >
                              <Download size={16} />
                          </button>
                      </div>
                  </div>
              </div>
          ))}
          {assets.length === 0 && (
            <div className="col-span-full p-12 border border-dashed border-slate-700 rounded-lg text-center text-slate-500">
                <Folder size={48} className="mx-auto mb-4 opacity-50" />
                <p>未找到数据资产。请运行同步任务以导入数据。</p>
            </div>
          )}
      </div>

      {/* Preview Modal */}
      <Modal isOpen={modalType === 'preview'} onClose={closeModal} title={`预览: ${selectedAsset?.name}`}>
        <div className="flex flex-col h-[70vh]">
            <div className="overflow-auto flex-1 border border-slate-700 rounded mb-4">
                {previewContent ? (
                    <table className="w-full text-left text-xs border-collapse">
                        <thead className="sticky top-0 z-10">
                            <tr className="bg-slate-800 text-slate-300 shadow-sm">
                                {selectedAsset?.type === 'table' && <th className="p-2 border border-slate-700 w-24 bg-slate-800">操作</th>}
                                {previewContent.columns.filter(c => c !== '_rowid').map(col => (
                                    <th key={col} className="p-2 border border-slate-700 bg-slate-800 whitespace-nowrap">{col}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {previewContent.data.map((row, i) => (
                                <tr key={i} className="hover:bg-slate-800/50 group">
                                    {selectedAsset?.type === 'table' && (
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
            
            {/* Pagination */}
            {previewContent && (
                <div className="flex justify-between items-center text-sm text-slate-400 border-t border-slate-700 pt-4">
                    <div>
                        显示 {((page - 1) * pageSize) + 1} - {Math.min(page * pageSize, total)} 共 {total} 行
                    </div>
                    <div className="flex items-center gap-2">
                        <button 
                            onClick={() => handlePageChange(page - 1)}
                            disabled={page <= 1}
                            className="p-1 rounded hover:bg-slate-800 disabled:opacity-50 disabled:hover:bg-transparent"
                        >
                            <ChevronLeft size={20} />
                        </button>
                        <span className="font-mono bg-slate-800 px-2 py-1 rounded">{page} / {totalPages || 1}</span>
                        <button 
                            onClick={() => handlePageChange(page + 1)}
                            disabled={page >= totalPages}
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
    </div>
  );
};

export default DataManagementPage;
