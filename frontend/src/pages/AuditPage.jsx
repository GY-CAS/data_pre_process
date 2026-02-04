import React, { useState, useEffect } from 'react';
import { ShieldAlert, Search, AlertTriangle } from 'lucide-react';
import { getAuditLogs } from '../api';

const AuditPage = () => {
  const [logs, setLogs] = useState([]);
  const [filters, setFilters] = useState({ user_id: '', action: '' });

  const fetchLogs = async () => {
    try {
      const params = {};
      if (filters.user_id) params.user_id = filters.user_id;
      if (filters.action) params.action = filters.action;
      
      const res = await getAuditLogs(params);
      setLogs(res.data);
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    fetchLogs();
  }, []); // Run once on mount

  const handleSearch = (e) => {
      e.preventDefault();
      fetchLogs();
  }

  const isWarning = (log) => {
      const warningKeywords = ['fail', 'error', 'delete', 'exception', 'warning'];
      const text = `${log.action} ${log.details}`.toLowerCase();
      return warningKeywords.some(k => text.includes(k));
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
          <ShieldAlert className="text-rose-500" /> 审计日志
        </h2>
      </div>

      <div className="bg-slate-900 border border-slate-700 rounded-lg p-4">
          <form onSubmit={handleSearch} className="flex gap-4 items-end">
              <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1">用户 ID</label>
                  <input 
                    type="text" 
                    className="bg-slate-950 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-rose-500"
                    value={filters.user_id}
                    onChange={e => setFilters({...filters, user_id: e.target.value})}
                    placeholder="按用户筛选..."
                  />
              </div>
              <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1">操作行为</label>
                  <input 
                    type="text" 
                    className="bg-slate-950 border border-slate-700 rounded px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-rose-500"
                    value={filters.action}
                    onChange={e => setFilters({...filters, action: e.target.value})}
                    placeholder="按行为筛选..."
                  />
              </div>
              <button type="submit" className="bg-slate-800 hover:bg-slate-700 text-slate-200 px-4 py-2 rounded flex items-center gap-2 transition-colors">
                  <Search size={16} /> 筛选
              </button>
          </form>
      </div>

      <div className="bg-slate-900 border border-slate-700 rounded-lg overflow-hidden">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-950 border-b border-slate-700 text-slate-400 text-sm uppercase tracking-wider">
              <th className="p-4 font-medium">时间</th>
              <th className="p-4 font-medium">用户</th>
              <th className="p-4 font-medium">行为</th>
              <th className="p-4 font-medium">资源</th>
              <th className="p-4 font-medium">详情</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {logs.map(log => {
              const warning = isWarning(log);
              return (
              <tr key={log.id} className={`hover:bg-slate-800/50 transition-colors ${warning ? 'bg-rose-950/10' : ''}`}>
                <td className="p-4 text-slate-500 text-sm font-mono whitespace-nowrap">
                    {new Date(log.timestamp).toLocaleString()}
                </td>
                <td className="p-4 font-medium text-slate-200">{log.user_id}</td>
                <td className="p-4 text-slate-300">
                    <span className={`px-2 py-1 rounded text-xs border flex items-center w-fit gap-1 ${
                        warning 
                        ? 'bg-rose-900/30 border-rose-800 text-rose-400' 
                        : 'bg-slate-800 border-slate-700'
                    }`}>
                        {warning && <AlertTriangle size={12} />}
                        {log.action}
                    </span>
                </td>
                <td className="p-4 text-slate-400 text-sm">{log.resource}</td>
                <td className="p-4 text-slate-500 text-sm">{log.details || '-'}</td>
              </tr>
            )})}
            {logs.length === 0 && (
              <tr>
                <td colSpan="5" className="p-8 text-center text-slate-500">未找到符合条件的日志。</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default AuditPage;
