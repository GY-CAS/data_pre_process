import React from 'react';
import { CheckCircle, XCircle, Clock, RefreshCw } from 'lucide-react';

export const Modal = ({ isOpen, onClose, title, children }) => {
  if (!isOpen) return null;
  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-slate-900 border border-slate-700 rounded-lg shadow-xl w-full max-w-lg overflow-hidden">
        <div className="px-6 py-4 border-b border-slate-700 flex justify-between items-center">
          <h3 className="text-lg font-semibold text-slate-100">{title}</h3>
          <button onClick={onClose} className="text-slate-400 hover:text-white">✕</button>
        </div>
        <div className="p-6">
          {children}
        </div>
      </div>
    </div>
  );
};

export const StatusBadge = ({ status }) => {
  const styles = {
    pending: "bg-slate-800 text-slate-300 border-slate-600",
    running: "bg-blue-900/30 text-blue-400 border-blue-800 animate-pulse",
    success: "bg-emerald-900/30 text-emerald-400 border-emerald-800",
    failed: "bg-rose-900/30 text-rose-400 border-rose-800",
  };
  
  const icons = {
    pending: <Clock size={14} />,
    running: <RefreshCw size={14} className="animate-spin" />,
    success: <CheckCircle size={14} />,
    failed: <XCircle size={14} />,
  };

  return (
    <span className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${styles[status] || styles.pending}`}>
      {icons[status] || icons.pending}
      {status.toUpperCase()}
    </span>
  );
};
