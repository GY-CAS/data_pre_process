import React, { useState, useEffect } from 'react';
import { Terminal, Plus, Play, AlertCircle, Loader2 } from 'lucide-react';
import { getTasks, createTask, runTask, getDataSources, getAuditLogs, getDataSourceMetadata } from '../api';
import { Modal, StatusBadge } from '../components/Common';

const TasksPage = () => {
  const [tasks, setTasks] = useState([]);
  const [sources, setSources] = useState([]);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [taskErrors, setTaskErrors] = useState({}); // Map task_id to error message
  const [formData, setFormData] = useState({ 
    name: '', 
    task_type: 'preprocess', 
    config: '' // Will be populated based on type
  });
  
  // Sync Task specific state
  const [syncDetails, setSyncDetails] = useState({
      sourceId: '',
      sourceTable: '',
      targetTable: '',
      mode: 'append'
  });
  const [sourceTables, setSourceTables] = useState([]);
  const [isLoadingMetadata, setIsLoadingMetadata] = useState(false);

  useEffect(() => {
    if (syncDetails.sourceId) {
        setIsLoadingMetadata(true);
        getDataSourceMetadata(syncDetails.sourceId)
            .then(res => setSourceTables(res.data.tables || []))
            .catch(err => console.error(err))
            .finally(() => setIsLoadingMetadata(false));
    } else {
        setSourceTables([]);
    }
  }, [syncDetails.sourceId]);

  const fetchTasks = async () => {
    try {
      const res = await getTasks();
      setTasks(res.data);
      // Fetch error details for failed tasks
      const failedTasks = res.data.filter(t => t.status === 'failed');
      failedTasks.forEach(t => fetchTaskError(t.id, t.name));
    } catch (err) {
      console.error(err);
    }
  };

  const fetchTaskError = async (taskId, taskName) => {
      try {
          // Look up audit logs for this task failure
          const res = await getAuditLogs({ action: 'task_failed', limit: 5 });
          // Simple matching strategy: find log where resource == taskName
          const errorLog = res.data.find(l => l.resource === taskName);
          if (errorLog) {
              setTaskErrors(prev => ({...prev, [taskId]: errorLog.details}));
          }
      } catch (err) {
          console.error(err);
      }
  };

  const fetchSources = async () => {
    try {
      const res = await getDataSources();
      setSources(res.data.data || []);
    } catch (err) {
        console.error(err);
    }
  }

  useEffect(() => {
    fetchTasks();
    fetchSources();
    const interval = setInterval(fetchTasks, 5000); // Poll every 5s
    return () => clearInterval(interval);
  }, []);

  // Update default config when task type changes
  useEffect(() => {
      if (formData.task_type === 'preprocess') {
          setFormData(prev => ({
              ...prev,
              config: JSON.stringify({
                job_name: "MyJob",
                source: { type: "csv", path: "data/input.csv" },
                operators: [{ type: "dedup" }],
                target: { type: "csv", path: "data/output", mode: "overwrite" }
              }, null, 2)
          }));
      } else if (formData.task_type === 'sync') {
           // Clear manual config, we'll build it from UI
           setFormData(prev => ({ ...prev, config: '' }));
      }
  }, [formData.task_type]);

  const handleRun = async (id) => {
    try {
      await runTask(id);
      fetchTasks();
    } catch (err) {
      alert('Failed to start task');
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      let finalConfig = formData.config;
      
      // If sync task, build config from UI
      if (formData.task_type === 'sync') {
          if (!syncDetails.sourceId || !syncDetails.sourceTable || !syncDetails.targetTable) {
              alert("Please complete all sync fields");
              return;
          }
          
          const configObj = {
              source_id: parseInt(syncDetails.sourceId),
              source: {
                  table: syncDetails.sourceTable
              },
              target: {
                  table: syncDetails.targetTable,
                  mode: syncDetails.mode
              }
          };
          finalConfig = JSON.stringify(configObj);
      }

      await createTask({
          ...formData,
          config: finalConfig
      });
      setIsModalOpen(false);
      fetchTasks();
    } catch (err) {
      alert('Failed to create task');
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
          <Terminal className="text-emerald-500" /> 任务管理
        </h2>
        <button 
          onClick={() => setIsModalOpen(true)}
          className="bg-emerald-600 hover:bg-emerald-500 text-white px-4 py-2 rounded-md flex items-center gap-2 transition-colors"
        >
          <Plus size={18} /> 创建任务
        </button>
      </div>

      <div className="bg-slate-900 border border-slate-700 rounded-lg overflow-hidden">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-950 border-b border-slate-700 text-slate-400 text-sm uppercase tracking-wider">
              <th className="p-4 font-medium">任务ID</th>
              <th className="p-4 font-medium">名称</th>
              <th className="p-4 font-medium">类型</th>
              <th className="p-4 font-medium">状态</th>
              <th className="p-4 font-medium text-right">操作</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {tasks.map(task => (
              <tr key={task.id} className="hover:bg-slate-800/50 transition-colors">
                <td className="p-4 text-slate-500 font-mono">#{task.id}</td>
                <td className="p-4 font-medium text-slate-200">{task.name}</td>
                <td className="p-4 text-slate-400">{task.task_type === 'sync' ? '同步' : '预处理'}</td>
                <td className="p-4">
                  <div className="flex flex-col gap-2">
                      <div className="flex items-center gap-2">
                        <StatusBadge status={task.status} />
                        {task.status === 'failed' && taskErrors[task.id] && (
                            <div className="relative group cursor-help">
                                <AlertCircle size={16} className="text-rose-500" />
                                <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 hidden group-hover:block w-64 p-2 bg-rose-950 border border-rose-800 rounded shadow-xl text-xs text-rose-200 z-10 break-words">
                                    {taskErrors[task.id]}
                                </div>
                            </div>
                        )}
                      </div>
                      {(task.task_type === 'sync' || task.progress > 0) && task.status !== 'pending' && (
                          <div className="w-full bg-slate-800 rounded-full h-1.5" title={`${task.progress || 0}%`}>
                                <div 
                                    className="bg-emerald-500 h-1.5 rounded-full transition-all duration-500" 
                                    style={{ width: `${task.progress || 0}%` }}
                                ></div>
                          </div>
                      )}
                  </div>
                </td>
                <td className="p-4 text-right">
                  <button 
                    onClick={() => handleRun(task.id)}
                    disabled={task.status === 'running'}
                    className="text-emerald-500 hover:text-emerald-400 disabled:opacity-50 disabled:cursor-not-allowed p-2 rounded hover:bg-emerald-950/50 transition-colors"
                    title="运行任务"
                  >
                    <Play size={18} />
                  </button>
                </td>
              </tr>
            ))}
            {tasks.length === 0 && (
                <tr>
                    <td colSpan="5" className="p-8 text-center text-slate-500">暂无任务，请点击右上角创建。</td>
                </tr>
            )}
          </tbody>
        </table>
      </div>

      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="创建新任务">
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-400 mb-1">任务名称</label>
            <input 
              type="text" 
              required
              className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
              value={formData.name}
              onChange={e => setFormData({...formData, name: e.target.value})}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-400 mb-1">任务类型</label>
            <select 
              className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
              value={formData.task_type}
              onChange={e => setFormData({...formData, task_type: e.target.value})}
            >
              <option value="preprocess">预处理 (Spark)</option>
              <option value="sync">全量同步</option>
            </select>
          </div>
          
          {formData.task_type === 'sync' ? (
             <div className="space-y-4 p-4 bg-slate-800/50 rounded border border-slate-700">
                 <h4 className="text-sm font-semibold text-slate-300">同步配置</h4>
                 <div>
                    <label className="block text-sm font-medium text-slate-400 mb-1">源数据库</label>
                    <select 
                      required
                      className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
                      value={syncDetails.sourceId}
                      onChange={e => setSyncDetails({...syncDetails, sourceId: e.target.value, sourceTable: ''})}
                    >
                      <option value="">选择数据源...</option>
                      {sources.filter(s => ['mysql', 'clickhouse', 'minio'].includes(s.type)).map(s => (
                          <option key={s.id} value={s.id}>{s.name} ({s.type})</option>
                      ))}
                    </select>
                 </div>
                 
                 {syncDetails.sourceId && (
                     <div>
                        <label className="block text-sm font-medium text-slate-400 mb-1">
                            {isLoadingMetadata ? (
                                <span className="flex items-center gap-2">加载表列表中... <Loader2 size={14} className="animate-spin"/></span>
                            ) : (
                                "源表 / Bucket"
                            )}
                        </label>
                        <select 
                          required
                          className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
                          value={syncDetails.sourceTable}
                          onChange={e => setSyncDetails({...syncDetails, sourceTable: e.target.value})}
                          disabled={isLoadingMetadata || sourceTables.length === 0}
                        >
                          <option value="">选择表/bucket...</option>
                          {sourceTables.map(t => (
                              <option key={t} value={t}>{t}</option>
                          ))}
                        </select>
                        {sourceTables.length === 0 && !isLoadingMetadata && (
                            <p className="text-xs text-amber-500 mt-1">未找到表或连接失败。</p>
                        )}
                     </div>
                 )}

                 <div>
                    <label className="block text-sm font-medium text-slate-400 mb-1">目标表 (系统数据库)</label>
                    <input 
                      type="text" 
                      required
                      placeholder="例如: synced_customers"
                      className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
                      value={syncDetails.targetTable}
                      onChange={e => setSyncDetails({...syncDetails, targetTable: e.target.value})}
                    />
                 </div>
                 <div>
                    <label className="block text-sm font-medium text-slate-400 mb-1">同步模式</label>
                    <select 
                      className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-emerald-500"
                      value={syncDetails.mode}
                      onChange={e => setSyncDetails({...syncDetails, mode: e.target.value})}
                    >
                      <option value="append">追加 (Append)</option>
                      <option value="overwrite">覆盖 (Overwrite)</option>
                    </select>
                 </div>
                 <p className="text-xs text-slate-500 mt-2">
                     * 数据将从选定的源同步到内部系统数据库。
                 </p>
             </div>
          ) : (
            <div>
                <label className="block text-sm font-medium text-slate-400 mb-1">配置 (JSON)</label>
                <textarea 
                className="w-full bg-slate-950 border border-slate-700 rounded px-3 py-2 text-slate-200 font-mono text-sm focus:outline-none focus:border-emerald-500 h-48"
                value={formData.config}
                onChange={e => setFormData({...formData, config: e.target.value})}
                />
            </div>
          )}

          <div className="flex justify-end gap-3 mt-6">
            <button type="button" onClick={() => setIsModalOpen(false)} className="px-4 py-2 text-slate-400 hover:text-white">取消</button>
            <button type="submit" className="bg-emerald-600 hover:bg-emerald-500 text-white px-4 py-2 rounded">创建任务</button>
          </div>
        </form>
      </Modal>
    </div>
  );
};

export default TasksPage;
