// Users page specific functionality
console.log('🚀 users.js loaded successfully!');
let allUsers = [];
let filteredUsers = [];
let currentPage = 1;
const usersPerPage = 10;

// Check if user is admin
function checkAdminAccess() {
  const user = localStorage.getItem('user');
  const userData = user ? JSON.parse(user) : null;
  console.log('🔍 Checking admin access for email:', userData?.email);
  if (!userData || userData.email !== 'klisaricf@gmail.com') {
    console.log('❌ Access denied - not admin user');
    window.location.href = '/';
    return false;
  }
  console.log('✅ Admin access granted');
  return true;
}

// Initialize users page - called from app.js
function initUsersPage() {
  console.log('📄 Initializing users page from app.js');
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
  
  // Load users
  console.log('📊 Loading users...');
  loadUsers();
  
  // Setup event listeners
  console.log('🎯 Setting up event listeners...');
  setupEventListeners();
}

// Make initUsersPage available globally
window.initUsersPage = initUsersPage;

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
  
  console.log('🔍 Search query:', query);
  console.log('📊 All users count:', allUsers.length);
  
  if (query) {
    clearBtn.style.display = 'flex';
    filteredUsers = allUsers.filter(user => 
      user.first_name.toLowerCase().includes(query) ||
      user.last_name.toLowerCase().includes(query) ||
      user.email.toLowerCase().includes(query)
    );
    console.log('✅ Filtered users count:', filteredUsers.length);
  } else {
    clearBtn.style.display = 'none';
    filteredUsers = [...allUsers];
    console.log('🔄 Reset to all users');
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
        <td data-label="ID">${globalIndex}</td>
        <td data-label="First Name">${user.first_name}</td>
        <td data-label="Last Name">${user.last_name}</td>
        <td data-label="Email">${user.email}</td>
        <td data-label="Registered">${registeredDate}</td>
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
  console.log('📄 Changing to page:', page, 'of', totalPages);
  if (page >= 1 && page <= totalPages) {
    currentPage = page;
    renderUsers();
    renderPagination();
    console.log('✅ Page changed successfully');
  } else {
    console.log('❌ Invalid page number');
  }
}

// Logout function
function logout() {
  localStorage.removeItem('token');
  localStorage.removeItem('userEmail');
  window.location.href = '/login';
}
