import { useState, useEffect } from 'react';
import './App.css';

function App() {
  const [showEditPopup, setShowEditPopup] = useState(false);
  // Track if popup is open for disabling buttons 
  const popupActive = showEditPopup;
  const [editRow, setEditRow] = useState(null);
  const [editForm, setEditForm] = useState({});
  const [editLoading, setEditLoading] = useState(false);
  const [editError, setEditError] = useState(null);
  const [editSuccess, setEditSuccess] = useState(null);
  const [aggregations, setAggregations] = useState(null);
  const [loadingAggregations, setLoadingAggregations] = useState(true);
  const [aggError, setAggError] = useState(null);

  // Distributed search state
  const [searchTerm, setSearchTerm] = useState('');
  const [limit, setLimit] = useState(20);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [hasSearched, setHasSearched] = useState(false);

  // Distributed select state
  const [selectColumn, setSelectColumn] = useState('averageRating');
  const [orderDirection, setOrderDirection] = useState('DESC');
  const [selectLimit, setSelectLimit] = useState(10);
  const [selectResults, setSelectResults] = useState([]);
  const [selectLoading, setSelectLoading] = useState(false);
  const [selectError, setSelectError] = useState(null);
  const [hasSelected, setHasSelected] = useState(false);

  const [deleteLoading, setDeleteLoading] = useState(false);

  // Add Reviews state
  const [showAddReviews, setShowAddReviews] = useState(false);
  const [reviewForm, setReviewForm] = useState({ newRating: '', newVotes: '' });
  const [reviewLoading, setReviewLoading] = useState(false);
  const [reviewError, setReviewError] = useState(null);
  const [reviewSuccess, setReviewSuccess] = useState(null);

  const handleDelete = async () => {
    if (!editForm.tconst) return;
    if (!window.confirm('Are you sure you want to delete this entry?')) return;
    setDeleteLoading(true);
    setEditError(null);
    try {
  const res = await fetch(`${process.env.REACT_APP_API_URL}/api/titles/distributed-delete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tconst: editForm.tconst }),
      });
      const data = await res.json();
      if (data.success) {
        setEditSuccess('Deleted successfully!');
        setTimeout(() => {
          setShowEditPopup(false);
          setEditRow(null);
          setEditSuccess(null);
          if (hasSearched) {
            handleSearch({ preventDefault: () => {} });
          }
          if (hasSelected) {
            handleSelect({ preventDefault: () => {} });
          }
          fetchAggregations();
        }, 800);
      } else {
        const errorMsg = data.message || data.error || 'Delete failed';
        console.error('Delete Error:', {
          operation: 'DELETE',
          tconst: editForm.tconst,
          error: errorMsg,
          fullResponse: data,
          timestamp: new Date().toISOString()
        });
        setEditError(`Delete Error: ${errorMsg}`);
      }
    } catch (err) {
      console.error('Delete Exception:', {
        operation: 'DELETE',
        tconst: editForm.tconst,
        error: err.message,
        stack: err.stack,
        timestamp: new Date().toISOString()
      });
      setEditError(`Delete Error: ${err.message || 'Delete failed'}`);
    } finally {
      setDeleteLoading(false);
    }
  };

  const handleAddReviews = async (e) => {
    e.preventDefault();
    if (!reviewForm.newRating || reviewForm.newRating.trim() === '') {
      setReviewError('Rating is required');
      return;
    }
    if (!reviewForm.newVotes || reviewForm.newVotes.trim() === '') {
      setReviewError('Number of votes is required');
      return;
    }
    
    const rating = parseFloat(reviewForm.newRating);
    const votes = parseInt(reviewForm.newVotes);
    
    if (isNaN(rating) || rating <= 0 || rating > 10) {
      setReviewError('Rating must be a positive number between 0 and 10');
      return;
    }
    if (isNaN(votes) || votes <= 0) {
      setReviewError('Number of votes must be a positive number');
      return;
    }
    
    setReviewLoading(true);
    setReviewError(null);
    setReviewSuccess(null);
    
    try {
      const res = await fetch(`${process.env.REACT_APP_API_URL}/api/titles/add-reviews`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tconst: editForm.tconst,
          newRating: rating,
          newVotes: votes
        }),
      });
      const data = await res.json();
      if (data.success) {
        setReviewSuccess('Reviews added successfully!');
        setReviewForm({ newRating: '', newVotes: '' });
        setTimeout(() => {
          setShowAddReviews(false);
          setShowEditPopup(false);
          setEditRow(null);
          setReviewSuccess(null);
          if (hasSearched) {
            handleSearch({ preventDefault: () => {} });
          }
          if (hasSelected) {
            handleSelect({ preventDefault: () => {} });
          }
          fetchAggregations();
        }, 1000);
      } else {
        const errorMsg = data.message || data.error || 'Failed to add reviews';
        console.error('Add Reviews Error:', {
          operation: 'ADD_REVIEWS',
          tconst: editForm.tconst,
          newRating: rating,
          newVotes: votes,
          error: errorMsg,
          fullResponse: data,
          timestamp: new Date().toISOString()
        });
        setReviewError(`Add Reviews Error: ${errorMsg}`);
      }
    } catch (err) {
      console.error('Add Reviews Exception:', {
        operation: 'ADD_REVIEWS',
        tconst: editForm.tconst,
        newRating: rating,
        newVotes: votes,
        error: err.message,
        stack: err.stack,
        timestamp: new Date().toISOString()
      });
      setReviewError(`Add Reviews Error: ${err.message || 'Failed to add reviews'}`);
    } finally {
      setReviewLoading(false);
    }
  };

  // When opening the popup, initialize editForm
  useEffect(() => {
    if (showEditPopup && editRow) {
      setEditForm({
        tconst: editRow.tconst || '',
        primaryTitle: editRow.primaryTitle || '',
        startYear: editRow.startYear || '',
        averageRating: editRow.averageRating || '',
        numVotes: editRow.numVotes || '',
        runtimeMinutes: editRow.runtimeMinutes || '',
        weightedRating: editRow.weightedRating || '',
      });
      setEditError(null);
      setEditSuccess(null);
      setShowAddReviews(false);
      setReviewForm({ newRating: '', newVotes: '' });
      setReviewError(null);
      setReviewSuccess(null);
    }
  }, [showEditPopup, editRow]);

  const handleEditFormChange = (e) => {
    const { name, value } = e.target;
    setEditForm((prev) => ({ ...prev, [name]: value }));
  };

  const handleEditSave = async (e) => {
    e.preventDefault();
    setEditLoading(true);
    setEditError(null);
    setEditSuccess(null);
    try {
      let url, body;
      if (editRow) {
        url = `${process.env.REACT_APP_API_URL}/api/titles/distributed-update`;
        body = JSON.stringify({
          tconst: editForm.tconst,
          primaryTitle: editForm.primaryTitle,
          runtimeMinutes: editForm.runtimeMinutes,
          averageRating: editForm.averageRating,
          numVotes: editForm.numVotes,
          startYear: editForm.startYear,
        });
      } else {
  url = `${process.env.REACT_APP_API_URL}/api/titles/distributed-insert`;
        body = JSON.stringify({
          tconst: editForm.tconst,
          primaryTitle: editForm.primaryTitle,
          runtimeMinutes: editForm.runtimeMinutes,
          averageRating: editForm.averageRating,
          numVotes: editForm.numVotes,
          startYear: editForm.startYear,
          weightedRating: editForm.weightedRating,
        });
      }
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
      });
      const data = await res.json();
      if (data.success) {
        setEditSuccess(editRow ? 'Update successful!' : 'Insert successful!');
        setTimeout(() => {
          setShowEditPopup(false);
          setEditRow(null);
          setEditSuccess(null);
          // Refresh all data
          if (hasSearched) {
            handleSearch({ preventDefault: () => {} });
          }
          if (hasSelected) {
            handleSelect({ preventDefault: () => {} });
          }
          fetchAggregations();
        }, 1000);
      } else {
        const errorMsg = data.message || data.error || (editRow ? 'Update failed' : 'Insert failed');
        console.error('Server Error:', {
          operation: editRow ? 'UPDATE' : 'INSERT',
          error: errorMsg,
          fullResponse: data,
          timestamp: new Date().toISOString()
        });
        setEditError(`Error: ${errorMsg}`);
      }
    } catch (err) {
      console.error('Edit/Insert error:', {
        operation: editRow ? 'UPDATE' : 'INSERT',
        error: err.message,
        stack: err.stack,
        timestamp: new Date().toISOString()
      });
      const errorMessage = err.message || (editRow ? 'Update failed' : 'Insert failed');
      setEditError(`Error: ${errorMessage}`);
    } finally {
      setEditLoading(false);
    }
  };

  const handleEditClick = (row) => {
    setEditRow(row);
    setShowEditPopup(true);
  };

  const handleEditPopupClose = () => {
    setShowEditPopup(false);
    setEditRow(null);
  };

  useEffect(() => {
    fetchAggregations();
  }, []);

  useEffect(() => {
    const nodeName = getNodeName();
    document.title = `Distributed IMDB - ${nodeName}`;
  }, []);

  const fetchAggregations = () => {
    setLoadingAggregations(true);
    setAggError(null);
  fetch(`${process.env.REACT_APP_API_URL}/api/aggregation`)
      .then((response) => response.json())
      .then((data) => {
        if (data.success) {
          setAggregations(data.data);
        } else {
          const errorMsg = data.message || data.error || 'Failed to load statistics';
          console.error('Aggregation Error:', {
            operation: 'AGGREGATION',
            error: errorMsg,
            fullResponse: data,
            timestamp: new Date().toISOString()
          });
          setAggError(`Aggregation Error: ${errorMsg}`);
        }
        setLoadingAggregations(false);
      })
      .catch((error) => {
        console.error('Aggregation Exception:', {
          operation: 'AGGREGATION',
          error: error.message,
          stack: error.stack,
          timestamp: new Date().toISOString()
        });
        setAggError(`Aggregation Error: ${error.message || 'Failed to load statistics'}`);
        setLoadingAggregations(false);
      });
  };

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!searchTerm || searchTerm.trim() === '') {
      setError('Please enter a search term');
      return;
    }
    
    if (!limit || limit < 1 || limit > 100) {
      setError('Please enter a valid limit (1-100)');
      return;
    }
    
    setLoading(true);
    setError(null);
    setHasSearched(true);

    try {
      const res = await fetch(
        `${process.env.REACT_APP_API_URL}/api/titles/distributed-search?search_term=${encodeURIComponent(
          searchTerm
        )}&limit_count=${limit}`
      );
      let data;
      let text;
      try {
        data = await res.clone().json();
      } catch (jsonErr) {
        text = await res.text();
        console.error('Failed to parse JSON.');
        console.error('Status:', res.status, res.statusText);
        console.error('Headers:', Array.from(res.headers.entries()));
        console.error('Raw response:', text);
        setError('Search failed: Invalid server response. See console for details.');
        return;
      }
      if (data.success) {
        setResults(data.data);
      } else {
        const errorMsg = data.message || 'Search failed';
        console.error('Search Error:', {
          operation: 'SEARCH',
          searchTerm,
          limit,
          error: errorMsg,
          fullResponse: data,
          timestamp: new Date().toISOString()
        });
        setError(`Search Error: ${errorMsg}`);
      }
    } catch (err) {
      console.error('Search Exception:', {
        operation: 'SEARCH',
        searchTerm,
        limit,
        error: err.message,
        stack: err.stack,
        timestamp: new Date().toISOString()
      });
      setError(`Search Error: ${err.message || 'Search failed'}`);
    } finally {
      setLoading(false);
    }
  };

  const handleSelect = async (e) => {
    e.preventDefault();
    if (!selectColumn || selectColumn.trim() === '') {
      setSelectError('Please select a column');
      return;
    }
    
    if (!orderDirection || (orderDirection !== 'ASC' && orderDirection !== 'DESC')) {
      setSelectError('Please select a valid order direction');
      return;
    }
    
    if (!selectLimit || selectLimit < 1 || selectLimit > 100) {
      setSelectError('Please enter a valid limit (1-100)');
      return;
    }
    
    setSelectLoading(true);
    setSelectError(null);
    setHasSelected(true);

    try {
      const res = await fetch(
        `${process.env.REACT_APP_API_URL}/api/titles/distributed-select?select_column=${encodeURIComponent(
          selectColumn
        )}&order_direction=${orderDirection}&limit_count=${selectLimit}`
      );
      const data = await res.json();
      if (data.success) {
        setSelectResults(data.data);
      } else {
        const errorMsg = data.message || 'Select failed';
        console.error('Select Error:', {
          operation: 'SELECT',
          selectColumn,
          orderDirection,
          selectLimit,
          error: errorMsg,
          fullResponse: data,
          timestamp: new Date().toISOString()
        });
        setSelectError(`Select Error: ${errorMsg}`);
      }
    } catch (err) {
      console.error('Select Exception:', {
        operation: 'SELECT',
        selectColumn,
        orderDirection,
        selectLimit,
        error: err.message,
        stack: err.stack,
        timestamp: new Date().toISOString()
      });
      setSelectError(`Select Error: ${err.message || 'Select failed'}`);
    } finally {
      setSelectLoading(false);
    }
  };

  const getNodeName = () => {
    const apiUrl = process.env.REACT_APP_API_URL || '';
    if (apiUrl.includes(':60751')) return 'Main Node';
    if (apiUrl.includes(':60752')) return 'Node A';
    if (apiUrl.includes(':60753')) return 'Node B';
    if (apiUrl.includes('stadvdb-mco2-main')) return 'Main Node';
    if (apiUrl.includes('stadvdb-mco2-node-a') || apiUrl.includes('stadvdb-mco2-a')) return 'Node A';
    if (apiUrl.includes('stadvdb-mco2-node-b') || apiUrl.includes('stadvdb-mco2-b')) return 'Node B';
    return 'Unknown Node';
  };

  return (
    <div className="App app-root">
      <header className="app-header">
        <div className="app-header-inner">
          <div>
            <h1 className="app-title">Distributed IMDB Database ({getNodeName()})</h1>
          </div>
        </div>
      </header>

      <main className="app-main">
        <section className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">Database Statistics</h2>
            </div>
            <button className="btn btn-ghost" onClick={fetchAggregations}>
              Refresh
            </button>
          </div>

          {aggError && <div className="alert alert-error">{aggError}</div>}

          {loadingAggregations ? (
            <p className="text-muted">Loading statistics…</p>
          ) : aggregations ? (
            <div className="stats-grid">
              {Object.entries(aggregations).map(([key, value]) => (
                <div key={key} className="stat-card">
                  <div className="stat-label">
                    {key
                      .replace(/_/g, ' ')
                      .replace(/\b\w/g, (l) => l.toUpperCase())}
                  </div>
                  <div className="stat-value">
                    {typeof value === 'number'
                      ? value.toLocaleString(undefined, {
                          maximumFractionDigits: 4,
                        })
                      : value}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-muted">No statistics available.</p>
          )}
        </section>

        <section className="card">
          <div className="card-header">
            <div>
              <h2 className="card-title">Distributed Search</h2>
            </div>
          </div>

          <form className="form-row" onSubmit={handleSearch}>
            <div className="form-group flex-2">
              <label className="field-label">Search Term</label>
              <input
                type="text"
                className="input"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                placeholder="e.g. The Matrix"
              />
            </div>

            <div className="form-group">
              <label className="field-label">Limit</label>
              <input
                type="number"
                className="input"
                value={limit}
                min={1}
                max={100}
                onChange={(e) => setLimit(Number(e.target.value))}
              />
            </div>

            <div className="form-group align-end">
              <button type="submit" className="btn btn-primary">
                {loading ? 'Searching…' : 'Search'}
              </button>
            </div>
          </form>

          {error && <div className="alert alert-error">{error}</div>}

          {!error && (
            <div className="table-wrapper" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 80 }}>
              {loading && (
                <p className="text-muted" style={{ margin: 0, textAlign: 'center', width: '100%' }}>Loading results…</p>
              )}

              {!loading && !hasSearched && (
                <p className="empty-state">Run a search to see results here.</p>
              )}

              {!loading && hasSearched && results.length === 0 && !error && (
                <p className="empty-state">No results found.</p>
              )}

              {!loading && results.length > 0 && (
                <table className="data-table">
                  <thead>
                    <tr>
                      {Object.keys(results[0]).map((col, idx) => (
                        <th key={idx}>{col}</th>
                      ))}
                      <th>Edit</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((row, i) => (
                      <tr key={i}>
                        {Object.values(row).map((val, j) => (
                          <td key={j}>{String(val)}</td>
                        ))}
                        <td>
                          <button
                            className="btn btn-secondary"
                            onClick={() => handleEditClick(row)}
                            disabled={popupActive}
                          >
                            Edit
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}
        </section>

        {showEditPopup && (
          <div
            className="modal-overlay"
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              width: '100vw',
              height: '100vh',
              background: 'rgba(0,0,0,0.4)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 1000,
            }}
            onClick={undefined}
          >
            <div
              className="modal-content"
              style={{
                background: '#fff',
                padding: 32,
                borderRadius: 8,
                minWidth: 350,
                maxWidth: 600,
                boxShadow: '0 2px 16px rgba(0,0,0,0.2)',
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <h3 style={{ marginTop: 0 }}>{editRow ? 'Edit Entry' : 'Add Title'}</h3>
              <form onSubmit={handleEditSave}>
                <div style={{ display: 'flex', gap: 12, marginBottom: 16 }}>
                  <div style={{ flex: 1 }}>
                    <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                      tconst
                    </label>
                    <input
                      type="text"
                      name="tconst"
                      value={editForm.tconst || ''}
                      disabled={!!editRow}
                      onChange={handleEditFormChange}
                      placeholder="e.g. tt1234567"
                      style={{
                        width: '100%',
                        padding: 8,
                        borderRadius: 4,
                        border: '1px solid #bbb',
                      }}
                      readOnly={!!editRow}
                    />
                  </div>
                  <div style={{ flex: 2 }}>
                    <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                      primaryTitle
                    </label>
                    <input
                      type="text"
                      name="primaryTitle"
                      value={editForm.primaryTitle || ''}
                      onChange={handleEditFormChange}
                      placeholder="e.g. The Matrix"
                      style={{
                        width: '100%',
                        padding: 8,
                        borderRadius: 4,
                        border: '1px solid #bbb',
                      }}
                    />
                  </div>
                  <div style={{ flex: 1 }}>
                    <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                      startYear
                    </label>
                    <input
                      type="text"
                      name="startYear"
                      value={editForm.startYear || ''}
                      onChange={handleEditFormChange}
                      placeholder="e.g. 1999"
                      style={{
                        width: '100%',
                        padding: 8,
                        borderRadius: 4,
                        border: '1px solid #bbb',
                      }}
                    />
                  </div>
                  <div style={{ flex: 1 }}>
                    <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                      runtimeMinutes
                    </label>
                    <input
                      type="text"
                      name="runtimeMinutes"
                      value={editForm.runtimeMinutes || ''}
                      onChange={handleEditFormChange}
                      placeholder="e.g. 120"
                      style={{
                        width: '100%',
                        padding: 8,
                        borderRadius: 4,
                        border: '1px solid #bbb',
                      }}
                    />
                  </div>
                </div>
                <div style={{ display: 'flex', gap: 12, marginBottom: 16 }}>
                  <div style={{ flex: 1 }}>
                    <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                      averageRating
                    </label>
                    <input
                      type="text"
                      name="averageRating"
                      value={editForm.averageRating || ''}
                      onChange={handleEditFormChange}
                      placeholder="e.g. 8.7"
                      style={{
                        width: '100%',
                        padding: 8,
                        borderRadius: 4,
                        border: '1px solid #bbb',
                      }}
                    />
                  </div>
                  <div style={{ flex: 1 }}>
                    <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                      numVotes
                    </label>
                    <input
                      type="text"
                      name="numVotes"
                      value={editForm.numVotes || ''}
                      onChange={handleEditFormChange}
                      placeholder="e.g. 10000"
                      style={{
                        width: '100%',
                        padding: 8,
                        borderRadius: 4,
                        border: '1px solid #bbb',
                      }}
                    />
                  </div>
                  {!!editRow && (
                    <div style={{ flex: 1 }}>
                      <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                        weightedRating
                      </label>
                      <input
                        type="text"
                        name="weightedRating"
                        value={editForm.weightedRating || ''}
                        disabled
                        style={{
                          width: '100%',
                          padding: 8,
                          borderRadius: 4,
                          border: '1px solid #bbb',
                          background: '#f5f5f5',
                        }}
                      />
                    </div>
                  )}
                </div>
                <input type="hidden" name="runtimeMinutes" value={editForm.runtimeMinutes || ''} />
                {editError && (
                  <div style={{ 
                    color: '#d32f2f', 
                    backgroundColor: '#ffebee',
                    padding: '12px',
                    borderRadius: '4px',
                    marginBottom: '8px',
                    border: '1px solid #ef5350',
                    fontWeight: '500'
                  }}>
                    {editError}
                  </div>
                )}
                {editSuccess && (
                  <div style={{ color: 'green', marginBottom: 8 }}>{editSuccess}</div>
                )}
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'flex-end',
                    gap: 8,
                  }}
                >
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={handleEditPopupClose}
                    disabled={editLoading || deleteLoading}
                  >
                    Close
                  </button>
                  {!!editRow && (
                    <button
                      type="button"
                      className="btn btn-danger"
                      style={{
                        background: (deleteLoading || editLoading) ? '#f5bdbd' : '#d32f2f',
                        color: (deleteLoading || editLoading) ? '#888' : '#fff',
                        border: 'none',
                        opacity: (deleteLoading || editLoading) ? 0.6 : 1,
                        cursor: (deleteLoading || editLoading) ? 'not-allowed' : 'pointer',
                      }}
                      onClick={handleDelete}
                      disabled={deleteLoading || editLoading}
                    >
                      {deleteLoading ? 'Deleting...' : 'Delete'}
                    </button>
                  )}
                  <button
                    type="submit"
                    className="btn btn-primary"
                    style={{ background: '#1976d2', color: '#fff', border: 'none' }}
                    disabled={editLoading || deleteLoading}
                  >
                    {editLoading ? 'Saving...' : !!editRow ? 'Edit' : 'Add'}
                  </button>
                </div>
              </form>

              {/* Add Reviews Section - Only shown when editing */}
              {!!editRow && (
                <div style={{ marginTop: 24, paddingTop: 24, borderTop: '1px solid #ddd' }}>
                  {!showAddReviews ? (
                    <button
                      type="button"
                      className="btn btn-secondary"
                      onClick={() => setShowAddReviews(true)}
                      disabled={editLoading || deleteLoading || reviewLoading}
                      style={{ width: '100%' }}
                    >
                      Add Reviews
                    </button>
                  ) : (
                    <>
                      <h4 style={{ marginTop: 0, marginBottom: 16 }}>Add Reviews</h4>
                      <form onSubmit={handleAddReviews}>
                        <div style={{ display: 'flex', gap: 12, marginBottom: 16 }}>
                          <div style={{ flex: 1 }}>
                            <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                              New Rating (0-10)
                            </label>
                            <input
                              type="number"
                              step="0.1"
                              min="0"
                              max="10"
                              value={reviewForm.newRating}
                              onChange={(e) => setReviewForm(prev => ({ ...prev, newRating: e.target.value }))}
                              placeholder="e.g. 8.5"
                              style={{
                                width: '100%',
                                padding: 8,
                                borderRadius: 4,
                                border: '1px solid #bbb',
                              }}
                              disabled={reviewLoading}
                            />
                          </div>
                          <div style={{ flex: 1 }}>
                            <label style={{ display: 'block', fontWeight: 500, marginBottom: 4 }}>
                              Number of Votes
                            </label>
                            <input
                              type="number"
                              min="1"
                              value={reviewForm.newVotes}
                              onChange={(e) => setReviewForm(prev => ({ ...prev, newVotes: e.target.value }))}
                              placeholder="e.g. 100"
                              style={{
                                width: '100%',
                                padding: 8,
                                borderRadius: 4,
                                border: '1px solid #bbb',
                              }}
                              disabled={reviewLoading}
                            />
                          </div>
                        </div>
                        {reviewError && (
                          <div style={{ 
                            color: '#d32f2f', 
                            backgroundColor: '#ffebee',
                            padding: '12px',
                            borderRadius: '4px',
                            marginBottom: '8px',
                            border: '1px solid #ef5350',
                            fontWeight: '500'
                          }}>
                            {reviewError}
                          </div>
                        )}
                        {reviewSuccess && <div style={{ color: 'green', marginBottom: 8 }}>{reviewSuccess}</div>}
                        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                          <button
                            type="button"
                            className="btn btn-secondary"
                            onClick={() => {
                              setShowAddReviews(false);
                              setReviewForm({ newRating: '', newVotes: '' });
                              setReviewError(null);
                              setReviewSuccess(null);
                            }}
                            disabled={reviewLoading}
                          >
                            Cancel
                          </button>
                          <button
                            type="submit"
                            className="btn btn-primary"
                            style={{ background: '#1976d2', color: '#fff', border: 'none' }}
                            disabled={reviewLoading}
                          >
                            {reviewLoading ? 'Adding...' : 'Add Reviews'}
                          </button>
                        </div>
                      </form>
                    </>
                  )}
                </div>
              )}
            </div>
          </div>
        )}

        <section className="card">
          <div
            className="card-header"
            style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}
          >
            <div>
              <h2 className="card-title">Distributed Select</h2>
            </div>
            <button
              className="btn btn-primary"
              style={{ minWidth: 120 }}
              onClick={() => {
                setEditRow(null);
                setEditForm({
                  tconst: '',
                  primaryTitle: '',
                  startYear: '',
                  averageRating: '',
                  numVotes: '',
                  runtimeMinutes: '',
                });
                setShowEditPopup(true);
              }}
              disabled={popupActive}
            >
              Add Title
            </button>
          </div>

          <form className="form-row" onSubmit={handleSelect}>
            <div className="form-group">
              <label className="field-label">Column</label>
              <select
                className="select"
                value={selectColumn}
                onChange={(e) => setSelectColumn(e.target.value)}
              >
                <option value="averageRating">Average Rating</option>
                <option value="weightedRating">Weighted Rating</option>
                <option value="numVotes">Vote Count</option>
                <option value="startYear">Release Year</option>
                <option value="primaryTitle">Title </option>
              </select>
            </div>
            <div className="form-group">
              <label className="field-label">Order</label>
              <select
                className="select"
                value={orderDirection}
                onChange={(e) => setOrderDirection(e.target.value)}
              >
                <option value="DESC">Descending</option>
                <option value="ASC">Ascending</option>
              </select>
            </div>
            <div className="form-group">
              <label className="field-label">Limit</label>
              <input
                type="number"
                className="input"
                value={selectLimit}
                min={1}
                max={100}
                onChange={(e) => setSelectLimit(Number(e.target.value))}
              />
            </div>
            <div className="form-group align-end">
              <button type="submit" className="btn btn-primary">
                {selectLoading ? 'Running…' : 'Select'}
              </button>
            </div>
          </form>

          {selectError && <div className="alert alert-error">{selectError}</div>}

          <div className="table-wrapper" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 80 }}>
            {selectLoading && (
              <p className="text-muted" style={{ margin: 0, textAlign: 'center', width: '100%' }}>Loading Results…</p>
            )}
            {!selectLoading && !hasSelected && (
              <p className="empty-state">Run a select to see results here.</p>
            )}
            {!selectLoading && hasSelected && selectResults.length === 0 && !selectError && (
              <p className="empty-state">No select results found.</p>
            )}
            {!selectLoading && selectResults.length > 0 && (
              <table className="data-table">
                <thead>
                  <tr>
                    {Object.keys(selectResults[0]).map((col, idx) => (
                      <th key={idx}>{col}</th>
                    ))}
                    <th>Edit</th>
                  </tr>
                </thead>
                <tbody>
                  {selectResults.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((val, j) => (
                        <td key={j}>{String(val)}</td>
                      ))}
                      <td>
                        <button
                          className="btn btn-secondary"
                          onClick={() => handleEditClick(row)}
                          disabled={popupActive}
                        >
                          Edit
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;
