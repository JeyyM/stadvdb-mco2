import { useState, useEffect } from 'react';
import './App.css';

// Failover fetch - tries primary backend, falls back to Main if it fails
const MAIN_BACKEND_URL = 'https://stadvdb-mco2-main.onrender.com';

async function fetchWithFailover(url, options = {}) {
  const primaryUrl = url.startsWith('http') ? url : `${process.env.REACT_APP_API_URL}${url}`;
  
  try {
    const response = await fetch(primaryUrl, options);
    if (response.ok) return response;
    throw new Error(`HTTP ${response.status}`);
  } catch (primaryError) {
    console.warn(`Primary backend failed (${primaryUrl}), trying Main...`, primaryError.message);
    
    // Build fallback URL to Main backend
    const fallbackUrl = url.startsWith('http') 
      ? url.replace(process.env.REACT_APP_API_URL, MAIN_BACKEND_URL)
      : `${MAIN_BACKEND_URL}${url}`;
    
    try {
      const response = await fetch(fallbackUrl, options);
      if (response.ok) {
        console.log('✓ Failover to Main backend successful');
        return response;
      }
      throw new Error(`HTTP ${response.status}`);
    } catch (fallbackError) {
      console.error('Both primary and Main backend failed');
      throw new Error(`All backends failed. Primary: ${primaryError.message}, Main: ${fallbackError.message}`);
    }
  }
}

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

  const handleDelete = async () => {
    if (!editForm.tconst) return;
    if (!window.confirm('Are you sure you want to delete this entry?')) return;
    setDeleteLoading(true);
    setEditError(null);
    try {
  const res = await fetchWithFailover(`/api/titles/distributed-delete`, {
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
          // Refresh all data
          if (hasSearched) {
            handleSearch({ preventDefault: () => {} });
          }
          if (hasSelected) {
            handleSelect({ preventDefault: () => {} });
          }
          fetchAggregations();
        }, 800);
      } else {
        setEditError(data.message || 'Delete failed');
      }
    } catch (err) {
      setEditError('Delete failed');
    } finally {
      setDeleteLoading(false);
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
        url = `/api/titles/distributed-update`;
        body = JSON.stringify({
          tconst: editForm.tconst,
          primaryTitle: editForm.primaryTitle,
          runtimeMinutes: editForm.runtimeMinutes,
          averageRating: editForm.averageRating,
          numVotes: editForm.numVotes,
          startYear: editForm.startYear,
        });
      } else {
  url = `/api/titles/distributed-insert`;
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
      const res = await fetchWithFailover(url, {
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
        setEditError(data.message || (editRow ? 'Update failed' : 'Insert failed'));
      }
    } catch (err) {
      setEditError(editRow ? 'Update failed' : 'Insert failed');
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Update page title based on connected node
  useEffect(() => {
    const nodeName = getNodeName();
    document.title = `Distributed IMDB - ${nodeName}`;
  }, []);

  const fetchAggregations = () => {
    setLoadingAggregations(true);
    setAggError(null);
  fetchWithFailover(`/api/aggregation`)
      .then((response) => response.json())
      .then((data) => {
        setAggregations(data.data);
        setLoadingAggregations(false);
      })
      .catch((error) => {
        setAggError('Failed to load statistics');
        setLoadingAggregations(false);
        console.error('Aggregation fetch error:', error);
      });
  };

  const handleSearch = async (e) => {
    e.preventDefault();
    
    // Validation checks
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
      const res = await fetchWithFailover(
        `/api/titles/distributed-search?search_term=${encodeURIComponent(
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
        setError(data.message || 'Search failed');
        console.error('API error:', data);
      }
    } catch (err) {
      console.error('Network or fetch error:', err);
      setError('Search failed');
    } finally {
      setLoading(false);
    }
  };

  const handleSelect = async (e) => {
    e.preventDefault();
    
    // Validation checks
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
      const res = await fetchWithFailover(
        `/api/titles/distributed-select?select_column=${encodeURIComponent(
          selectColumn
        )}&order_direction=${orderDirection}&limit_count=${selectLimit}`
      );
      const data = await res.json();
      if (data.success) {
        setSelectResults(data.data);
      } else {
        setSelectError(data.message || 'Select failed');
      }
    } catch (err) {
      console.error(err);
      setSelectError('Select failed');
    } finally {
      setSelectLoading(false);
    }
  };

  // Determine which node we're connected to based on the API URL port or domain
  const getNodeName = () => {
    const apiUrl = process.env.REACT_APP_API_URL || '';
    // Check for localhost ports
    if (apiUrl.includes(':60751')) return 'Main Node';
    if (apiUrl.includes(':60752')) return 'Node A';
    if (apiUrl.includes(':60753')) return 'Node B';
    // Check for Render URLs
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
                {editError && <div style={{ color: 'red', marginBottom: 8 }}>{editError}</div>}
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
