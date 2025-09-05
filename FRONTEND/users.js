// Users page specific functionality
console.log('🚀 users.js loaded successfully!');
let allUsers = [];
let filteredUsers = [];
let currentPage = 1;
const usersPerPage = 10;

// Check if user is admin
function checkAdminAccess() {
  const userEmail = localStorage.getItem('userEmail');
  console.log('🔍 Checking admin access for email:', userEmail);
  if (userEmail !== 'klisaricf@gmail.com') {
    console.log('❌ Access denied - not admin user');
    window.location.href = '/';
    return false;
  }
  console.log('✅ Admin access granted');
  return true;
}

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
  console.log('📄 DOM Content Loaded - initializing users page');
  if (!checkAdminAccess()) return;
  
  console.log('🔧 Setting up admin UI elements');
  // Show admin info
  const adminInfo = document.getElementById('adminInfo');
  const adminEmail = document.getElementById('adminEmail');
  const logoutBtn = document.getElementById('logoutBtn');
  
  if (adminInfo) {
    adminInfo.style.display = 'flex';
    console.log('✅ Admin info shown');
  } else {
    console.log('❌ Admin info element not found');
  }
  
  if (adminEmail) {
    adminEmail.textContent = localStorage.getItem('userEmail');
    console.log('✅ Admin email set');
  } else {
    console.log('❌ Admin email element not found');
  }
  
  if (logoutBtn) {
    logoutBtn.style.display = 'flex';
    console.log('✅ Logout button shown');
  } else {
    console.log('❌ Logout button not found');
  }
  
  // Hide controls section
  hideControlsSection();
  
  // Load users
  console.log('📊 Loading users...');
  loadUsers();
  
  // Setup event listeners
  console.log('🎯 Setting up event listeners...');
  setupEventListeners();
  
  // Setup responsive table
  setupResponsiveTable();
});

// Setup event listeners
function setupEventListeners() {
  // Search functionality
  const searchInput = document.getElementById('userSearch');
  const clearSearch = document.getElementById('clearSearch');
  
  searchInput.addEventListener('input', handleSearch);
  clearSearch.addEventListener('click', clearSearchInput);
  
  // Pagination
  document.getElementById('prevPage').addEventListener('click', () => changePage(currentPage - 1));
  document.getElementById('nextPage').addEventListener('click', () => changePage(currentPage + 1));
  
  // Logout
  document.getElementById('logoutBtn').addEventListener('click', logout);
}

// Load users from API
async function loadUsers() {
  console.log('🔄 Starting to load users...');
  const loading = document.getElementById('tableLoading');
  const error = document.getElementById('tableError');
  const table = document.getElementById('usersTable');
  
  console.log('📋 UI elements found:', {
    loading: !!loading,
    error: !!error,
    table: !!table
  });
  
  if (loading) loading.style.display = 'flex';
  if (error) error.style.display = 'none';
  if (table) table.style.display = 'none';
  
  try {
    const token = localStorage.getItem('token');
    console.log('🔑 Token found:', !!token);
    
    console.log('🌐 Making API request to /api/users...');
    const response = await fetch('/api/users', {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });
    
    console.log('📡 API response status:', response.status);
    
    if (!response.ok) {
      const errorText = await response.text();
      console.log('❌ API error response:', errorText);
      throw new Error(`Failed to load users: ${response.status} ${errorText}`);
    }
    
    const data = await response.json();
    console.log('📊 Users data received:', data);
    
    allUsers = data.users || [];
    filteredUsers = [...allUsers];
    
    console.log('👥 Users loaded:', allUsers.length);
    
    updateStats();
    renderUsers();
    renderPagination();
    
    if (loading) loading.style.display = 'none';
    if (table) table.style.display = 'table';
    
    console.log('✅ Users page fully loaded');
    
  } catch (err) {
    console.error('❌ Error loading users:', err);
    if (loading) loading.style.display = 'none';
    if (error) error.style.display = 'flex';
    const errorMessage = document.getElementById('errorMessage');
    if (errorMessage) errorMessage.textContent = err.message;
  }
}

// Handle search
function handleSearch(e) {
  const query = e.target.value.toLowerCase().trim();
  const clearBtn = document.getElementById('clearSearch');
  
  if (query) {
    clearBtn.style.display = 'flex';
    filteredUsers = allUsers.filter(user => 
      user.first_name.toLowerCase().includes(query) ||
      user.last_name.toLowerCase().includes(query) ||
      user.email.toLowerCase().includes(query)
    );
  } else {
    clearBtn.style.display = 'none';
    filteredUsers = [...allUsers];
  }
  
  currentPage = 1;
  updateStats();
  renderUsers();
  renderPagination();
}

// Clear search
function clearSearchInput() {
  document.getElementById('userSearch').value = '';
  document.getElementById('clearSearch').style.display = 'none';
  filteredUsers = [...allUsers];
  currentPage = 1;
  updateStats();
  renderUsers();
  renderPagination();
}

// Update statistics
function updateStats() {
  document.getElementById('totalUsers').textContent = allUsers.length;
  document.getElementById('showingUsers').textContent = filteredUsers.length;
}

// Render users table
function renderUsers() {
  const tbody = document.getElementById('usersTableBody');
  const noUsers = document.getElementById('noUsers');
  const pagination = document.getElementById('paginationContainer');
  
  if (filteredUsers.length === 0) {
    tbody.innerHTML = '';
    noUsers.style.display = 'flex';
    pagination.style.display = 'none';
    return;
  }
  
  noUsers.style.display = 'none';
  pagination.style.display = 'flex';
  
  const startIndex = (currentPage - 1) * usersPerPage;
  const endIndex = startIndex + usersPerPage;
  const pageUsers = filteredUsers.slice(startIndex, endIndex);
  
  tbody.innerHTML = pageUsers.map((user, index) => {
    const globalIndex = startIndex + index + 1;
    const registeredDate = new Date(user.created_at).toLocaleDateString('sr-RS');
    
    return `
      <tr>
        <td>${globalIndex}</td>
        <td>${user.first_name}</td>
        <td>${user.last_name}</td>
        <td>${user.email}</td>
        <td>${registeredDate}</td>
      </tr>
    `;
  }).join('');
}

// Render pagination
function renderPagination() {
  const totalPages = Math.ceil(filteredUsers.length / usersPerPage);
  const paginationInfo = document.getElementById('paginationInfo');
  const paginationPages = document.getElementById('paginationPages');
  const prevBtn = document.getElementById('prevPage');
  const nextBtn = document.getElementById('nextPage');
  
  if (totalPages <= 1) {
    document.getElementById('paginationContainer').style.display = 'none';
    return;
  }
  
  paginationInfo.textContent = `Page ${currentPage} of ${totalPages}`;
  
  // Previous button
  prevBtn.disabled = currentPage === 1;
  
  // Next button
  nextBtn.disabled = currentPage === totalPages;
  
  // Page numbers
  paginationPages.innerHTML = '';
  const maxVisiblePages = 5;
  let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
  let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
  
  if (endPage - startPage + 1 < maxVisiblePages) {
    startPage = Math.max(1, endPage - maxVisiblePages + 1);
  }
  
  for (let i = startPage; i <= endPage; i++) {
    const pageBtn = document.createElement('button');
    pageBtn.className = `btn pagination-page ${i === currentPage ? 'active' : ''}`;
    pageBtn.textContent = i;
    pageBtn.addEventListener('click', () => changePage(i));
    paginationPages.appendChild(pageBtn);
  }
}

// Change page
function changePage(page) {
  const totalPages = Math.ceil(filteredUsers.length / usersPerPage);
  if (page >= 1 && page <= totalPages) {
    currentPage = page;
    renderUsers();
    renderPagination();
  }
}

// Logout function
function logout() {
  localStorage.removeItem('token');
  localStorage.removeItem('userEmail');
  window.location.href = '/login';
}

// Hide controls section
function hideControlsSection() {
  console.log('🚫 Hiding controls section...');
  
  // Function to hide controls panel
  const hideControls = () => {
    const controlsPanel = document.querySelector('.panel:has(.controls__row)');
    if (controlsPanel) {
      controlsPanel.style.display = 'none';
      console.log('✅ Controls panel hidden');
    }
  };
  
  // Hide immediately if already exists
  hideControls();
  
  // Watch for dynamically added controls panel
  const observer = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
      if (mutation.type === 'childList') {
        mutation.addedNodes.forEach((node) => {
          if (node.nodeType === Node.ELEMENT_NODE) {
            if (node.classList && node.classList.contains('panel')) {
              const controlsRow = node.querySelector('.controls__row');
              if (controlsRow) {
                node.style.display = 'none';
                console.log('✅ Dynamically added controls panel hidden');
              }
            }
          }
        });
      }
    });
  });
  
  // Start observing
  observer.observe(document.body, {
    childList: true,
    subtree: true
  });
  
  console.log('👀 Started watching for controls panel');
}

// Setup responsive table
function setupResponsiveTable() {
  console.log('📱 Setting up responsive table...');
  
  const table = document.getElementById('usersTable');
  if (!table) return;
  
  // Add responsive wrapper
  const wrapper = document.createElement('div');
  wrapper.className = 'table-responsive-wrapper';
  wrapper.style.overflowX = 'auto';
  wrapper.style.width = '100%';
  
  // Wrap the table
  table.parentNode.insertBefore(wrapper, table);
  wrapper.appendChild(table);
  
  // Add responsive styles
  const style = document.createElement('style');
  style.textContent = `
    .table-responsive-wrapper {
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
    }
    
    @media (max-width: 768px) {
      .users-table {
        min-width: 600px;
        font-size: 0.875rem;
      }
      
      .users-table th,
      .users-table td {
        padding: 0.5rem 0.25rem;
        white-space: nowrap;
      }
      
      .users-table th:first-child,
      .users-table td:first-child {
        position: sticky;
        left: 0;
        background: var(--surface);
        z-index: 1;
        border-right: 1px solid var(--border);
      }
    }
    
    @media (max-width: 640px) {
      .users-table {
        min-width: 500px;
        font-size: 0.75rem;
      }
      
      .users-table th,
      .users-table td {
        padding: 0.25rem 0.125rem;
      }
    }
    
    @media (max-width: 480px) {
      .users-table {
        min-width: 400px;
        font-size: 0.625rem;
      }
      
      .users-table th,
      .users-table td {
        padding: 0.125rem 0.0625rem;
      }
    }
  `;
  
  document.head.appendChild(style);
  
  console.log('✅ Responsive table setup complete');
}
