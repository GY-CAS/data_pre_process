import React, { useState, useEffect } from 'react';
import { ShieldAlert, Search, AlertTriangle, Trash2, ChevronLeft, ChevronRight } from 'lucide-react';
import { getAuditLogs, deleteAuditLogs } from '../api';

const AuditPage = () => {
  const [logs, setLogs] = useState([]);
  const [filters, setFilters] = useState({ user_id: '', action: '' });
  
  // Pagination state
  const [page, setPage] = useState(1);
  const [pageSize] = useState(10);
  const [total, setTotal] = useState(0);
  
  // Selection state
  const [selectedIds, setSelectedIds] = useState([]);

  const fetchLogs = async () => {
    try {
      const params = {
          skip: (page - 1) * pageSize,
          limit: pageSize
      };
      if (filters.user_id) params.user_id = filters.user_id;
      if (filters.action) params.action = filters.action;
      
      const res = await getAuditLogs(params);
      // Support both old and new response structure just in case, but we know it's new
      const items = res.data.items || res.data;
      const totalCount = res.data.total || (Array.isArray(res.data) ? res.data.length : 0);
      
      setLogs(items);
      setTotal(totalCount);
      // Clear selection on refresh/page change if desired, or keep it? 
      // Usually clear on page change is safer to avoid confusion
      setSelectedIds([]); 
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    fetchLogs();
  }, [page, pageSize]); // Refetch when page changes

  const handleSearch = (e) => {
      e.preventDefault();
      setPage(1); // Reset to first page on search
      fetchLogs();
  }

  const handleSelectAll = (e) => {
      if (e.target.checked) {
          setSelectedIds(logs.map(log => log.id));
      } else {
          setSelectedIds([]);
      }
  };

  const handleSelectOne = (id) => {
      if (selectedIds.includes(id)) {
          setSelectedIds(selectedIds.filter(sid => sid !== id));
      } else {
          setSelectedIds([...selectedIds, id]);
      }
  };

  const handleBulkDelete = async () => {
      if (selectedIds.length === 0) return;
      
      if (confirm(`确认删除选中的 ${selectedIds.length} 条日志?`)) {
          try {
              await deleteAuditLogs(selectedIds);
              fetchLogs();
              setSelectedIds([]);
          } catch (err) {
              alert('删除失败');
              console.error(err);
          }
      }
  };

  const isWarning = (log) => {
      const warningKeywords = ['fail', 'error', 'delete', 'exception', 'warning'];
      const text = `${log.action} ${log.details}`.toLowerCase();
      return warningKeywords.some(k => text.includes(k));
  };

  const totalPages = Math.ceil(total / pageSize);

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
          <ShieldAlert className="text-rose-500" /> 审计日志
        </h2>
        
        {selectedIds.length > 0 && (
            <button 
                onClick={handleBulkDelete}
                className="bg-rose-50 border border-rose-200 hover:bg-rose-100 text-rose-600 px-4 py-2 rounded flex items-center gap-2 transition-colors"
            >
                <Trash2 size={16} /> 删除选中 ({selectedIds.length})
            </button>
        )}
      </div>

      <div className="bg-white border border-slate-200 rounded-lg p-4 shadow-sm">
          <form onSubmit={handleSearch} className="flex gap-4 items-end">
              <div>
                  <label className="block text-xs font-medium text-slate-500 mb-1">用户 ID</label>
                  <input 
                    type="text" 
                    className="bg-slate-50 border border-slate-200 rounded px-3 py-2 text-sm text-slate-700 focus:outline-none focus:border-rose-500 focus:ring-1 focus:ring-rose-500/20"
                    value={filters.user_id}
                    onChange={e => setFilters({...filters, user_id: e.target.value})}
                    placeholder="按用户筛选..."
                  />
              </div>
              <div>
                  <label className="block text-xs font-medium text-slate-500 mb-1">操作行为</label>
                  <input 
                    type="text" 
                    className="bg-slate-50 border border-slate-200 rounded px-3 py-2 text-sm text-slate-700 focus:outline-none focus:border-rose-500 focus:ring-1 focus:ring-rose-500/20"
                    value={filters.action}
                    onChange={e => setFilters({...filters, action: e.target.value})}
                    placeholder="按行为筛选..."
                  />
              </div>
              <button type="submit" className="bg-slate-800 hover:bg-slate-700 text-white px-4 py-2 rounded flex items-center gap-2 transition-colors shadow-sm">
                  <Search size={16} /> 筛选
              </button>
          </form>
      </div>

      <div className="bg-white border border-slate-200 rounded-lg shadow-sm flex-1 overflow-y-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-50 border-b border-slate-200 text-slate-500 text-sm uppercase tracking-wider sticky top-0 z-10">
              <th className="p-4 w-10">
                  <input 
                    type="checkbox" 
                    className="rounded border-slate-300 bg-white text-rose-500 focus:ring-rose-500/30"
                    checked={logs.length > 0 && selectedIds.length === logs.length}
                    onChange={handleSelectAll}
                  />
              </th>
              <th className="p-4 font-medium">用户</th>
              <th className="p-4 font-medium">行为</th>
              <th className="p-4 font-medium">资源</th>
              <th className="p-4 font-medium">详情</th>
              <th className="p-4 font-medium">时间</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {logs.map(log => {
              const warning = isWarning(log);
              return (
              <tr key={log.id} className={`hover:bg-slate-50 transition-colors ${warning ? 'bg-rose-50/50' : ''}`}>
                <td className="p-4">
                    <input 
                        type="checkbox" 
                        className="rounded border-slate-300 bg-white text-rose-500 focus:ring-rose-500/30"
                        checked={selectedIds.includes(log.id)}
                        onChange={() => handleSelectOne(log.id)}
                    />
                </td>
                <td className="p-4 font-medium text-slate-700">{log.user_id}</td>
                <td className="p-4 text-slate-600">
                    <span className={`px-2 py-1 rounded text-xs border flex items-center w-fit gap-1 ${
                        warning 
                        ? 'bg-rose-50 border-rose-200 text-rose-600' 
                        : 'bg-slate-100 border-slate-200'
                    }`}>
                        {warning && <AlertTriangle size={12} />}
                        {log.action}
                    </span>
                </td>
                <td className="p-4 text-slate-600 text-sm">{log.resource}</td>
                <td className="p-4 text-slate-500 text-sm">{log.details || '-'}</td>
                <td className="p-4 text-slate-500 text-sm font-mono whitespace-nowrap">
                    {new Date(log.timestamp + 'Z').toLocaleString()}
                </td>
              </tr>
            )})}
            {logs.length === 0 && (
              <tr>
                <td colSpan="6" className="p-8 text-center text-slate-400">未找到符合条件的日志。</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      
      {/* Pagination Controls */}
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
                      max={Math.ceil(total / pageSize)}
                      className="w-12 h-8 text-center bg-white border border-slate-200 rounded text-sm text-slate-700 focus:outline-none focus:border-rose-500"
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
                                  ? 'bg-rose-600 text-white shadow-sm' 
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
    </div>
  );
};

export default AuditPage;
