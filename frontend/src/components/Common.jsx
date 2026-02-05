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
    pending: "bg-slate-100 text-slate-600 border-slate-300",
    running: "bg-blue-50 text-blue-600 border-blue-200 animate-pulse",
    success: "bg-emerald-50 text-emerald-600 border-emerald-200",
    failed: "bg-rose-50 text-rose-600 border-rose-200",
  };
  
  const icons = {
    pending: <Clock size={14} strokeWidth={2.5} />,
    running: <RefreshCw size={14} className="animate-spin" strokeWidth={2.5} />,
    success: <CheckCircle size={14} strokeWidth={2.5} />,
    failed: <XCircle size={14} strokeWidth={2.5} />,
  };

  return (
    <span className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-bold border ${styles[status] || styles.pending}`}>
      {icons[status] || icons.pending}
      {status.toUpperCase()}
    </span>
  );
};
