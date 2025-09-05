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
  
  // Handle window resize for mobile layout
  window.addEventListener('resize', updateMobileLayout);
}

// Load users from API with pagination
async function loadUsers(page = 1, searchQuery = '') {
  console.log('🔄 Starting to load users...', { page, searchQuery });
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
    const user = localStorage.getItem('user');
    const userData = user ? JSON.parse(user) : null;
    const sessionId = userData?.session_id;
    console.log('🔑 Session ID found:', !!sessionId);
    
    // Build API URL with pagination and search parameters
    const apiUrl = new URL('/api/users', window.location.origin);
    apiUrl.searchParams.set('page', page);
    apiUrl.searchParams.set('limit', usersPerPage);
    if (searchQuery) {
      apiUrl.searchParams.set('search', searchQuery);
    }
    
    console.log('🌐 API URL with params:', apiUrl.toString());
    
    console.log('🌐 Making API request to:', apiUrl.toString());
    const response = await fetch(apiUrl.toString(), {
      headers: {
        'Authorization': `Bearer ${sessionId}`
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
    
    // Update global state
    allUsers = data.users || [];
    filteredUsers = [...allUsers];
    currentPage = page;
    
    // Update total count for pagination
    window.totalUsersCount = data.total || allUsers.length;
    
    console.log('👥 Users loaded:', allUsers.length);
    console.log('📊 Total users in database:', window.totalUsersCount);
    console.log('🔍 All users array:', allUsers);
    
    console.log('📊 Updating stats...');
    updateStats();
    
    console.log('🎨 Rendering users table...');
    renderUsers();
    
    console.log('📄 Rendering pagination...');
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

// Handle search with debouncing
let searchTimeout;
function handleSearch(e) {
  const query = e.target.value.trim();
  const clearBtn = document.getElementById('clearSearch');
  
  console.log('🔍 Search query:', query);
  
  // Clear previous timeout
  if (searchTimeout) {
    clearTimeout(searchTimeout);
  }
  
  // Show clear button if there's a query
  if (query) {
    clearBtn.style.display = 'flex';
  } else {
    clearBtn.style.display = 'none';
  }
  
  // Debounce search - wait 300ms after user stops typing
  searchTimeout = setTimeout(() => {
    currentPage = 1;
    
    if (query) {
      // For now, use client-side search since backend might not support search params
      console.log('🔍 Using client-side search for:', query);
      performClientSideSearch(query);
    } else {
      // No search query, load first page
      loadUsers(1, '');
    }
  }, 300);
}

// Client-side search
function performClientSideSearch(query) {
  console.log('🔍 Performing client-side search for:', query);
  
  // Always load all users first to ensure we have complete data
  loadAllUsers().then(() => {
    filterUsers(query);
  }).catch(() => {
    console.error('❌ Failed to load users for search');
  });
}

// Load all users for client-side search
async function loadAllUsers() {
  console.log('📥 Loading all users for client-side search...');
  const user = localStorage.getItem('user');
  const userData = user ? JSON.parse(user) : null;
  const sessionId = userData?.session_id;
  
  const response = await fetch('/api/users', {
    headers: {
      'Authorization': `Bearer ${sessionId}`
    }
  });
  
  if (response.ok) {
    const data = await response.json();
    allUsers = data.users || [];
    console.log('✅ All users loaded for search:', allUsers.length);
  } else {
    console.error('❌ Failed to load all users');
  }
}

// Filter users client-side
function filterUsers(query) {
  console.log('🔍 Filtering users client-side:', query);
  
  filteredUsers = allUsers.filter(user => {
    const fullName = `${user.first_name} ${user.last_name}`.toLowerCase();
    return user.first_name.toLowerCase().includes(query.toLowerCase()) ||
           user.last_name.toLowerCase().includes(query.toLowerCase()) ||
           fullName.includes(query.toLowerCase()) ||
           user.email.toLowerCase().includes(query.toLowerCase());
  });
  
  console.log('✅ Filtered users:', filteredUsers.length);
  
  currentPage = 1;
  updateStats();
  renderUsers();
  renderPagination();
}

// Clear search
function clearSearchInput() {
  document.getElementById('userSearch').value = '';
  document.getElementById('clearSearch').style.display = 'none';
  
  // Clear timeout
  if (searchTimeout) {
    clearTimeout(searchTimeout);
  }
  
  // Reload first page without search
  currentPage = 1;
  loadUsers(1, '');
}

// Update statistics
function updateStats() {
  const totalUsers = window.totalUsersCount || allUsers.length;
  const showingUsers = allUsers.length;
  
  document.getElementById('totalUsers').textContent = totalUsers;
  document.getElementById('showingUsers').textContent = showingUsers;
  
  console.log('📊 Stats updated:', { totalUsers, showingUsers });
}

// Render users table
function renderUsers() {
  console.log('🎨 renderUsers called');
  console.log('📊 filteredUsers.length:', filteredUsers.length);
  console.log('📄 currentPage:', currentPage);
  console.log('📊 usersPerPage:', usersPerPage);
  
  const tbody = document.getElementById('usersTableBody');
  const mobileCards = document.getElementById('usersMobileCards');
  const noUsers = document.getElementById('noUsers');
  const pagination = document.getElementById('paginationContainer');
  
  console.log('🔍 DOM elements found:', {
    tbody: !!tbody,
    mobileCards: !!mobileCards,
    noUsers: !!noUsers,
    pagination: !!pagination
  });
  
  if (filteredUsers.length === 0) {
    console.log('❌ No users to display');
    if (tbody) tbody.innerHTML = '';
    if (mobileCards) mobileCards.innerHTML = '';
    if (noUsers) noUsers.style.display = 'flex';
    if (pagination) pagination.style.display = 'none';
    return;
  }
  
  if (noUsers) noUsers.style.display = 'none';
  if (pagination) pagination.style.display = 'flex';
  
  const startIndex = (currentPage - 1) * usersPerPage;
  const endIndex = startIndex + usersPerPage;
  const pageUsers = filteredUsers.slice(startIndex, endIndex);
  
  console.log('📊 Page users:', pageUsers.length);
  console.log('🔍 Page users data:', pageUsers);
  
  // Render desktop table
  if (tbody) {
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
    console.log('✅ Table rendered successfully');
  } else {
    console.log('❌ tbody element not found');
  }

  // Render mobile cards
  if (mobileCards) {
    mobileCards.innerHTML = pageUsers.map((user, index) => {
      const globalIndex = startIndex + index + 1;
      const registeredDate = new Date(user.created_at).toLocaleDateString('sr-RS');
      
      return `
        <div class="user-card">
          <div class="user-card-header">
            <div class="user-id">#${globalIndex}</div>
          </div>
          <div class="user-details">
            <div class="user-detail">
              <div class="user-detail-label">First Name</div>
              <div class="user-detail-value">${user.first_name}</div>
            </div>
            <div class="user-detail">
              <div class="user-detail-label">Last Name</div>
              <div class="user-detail-value">${user.last_name}</div>
            </div>
            <div class="user-detail">
              <div class="user-detail-label">Email</div>
              <div class="user-detail-value">${user.email}</div>
            </div>
            <div class="user-detail">
              <div class="user-detail-label">Registered</div>
              <div class="user-detail-value">${registeredDate}</div>
            </div>
          </div>
        </div>
      `;
    }).join('');
    console.log('✅ Mobile cards rendered successfully');
  }

  // Update mobile layout
  updateMobileLayout();
}

// Update mobile layout based on screen size
function updateMobileLayout() {
  const table = document.getElementById('usersTable');
  const mobileCards = document.getElementById('usersMobileCards');
  
  if (!table || !mobileCards) return;

  if (window.innerWidth <= 768) {
    table.style.display = 'none';
    mobileCards.style.display = 'block';
  } else {
    table.style.display = 'table';
    mobileCards.style.display = 'none';
  }
}

// Render pagination
function renderPagination() {
  const totalUsers = window.totalUsersCount || filteredUsers.length;
  const totalPages = Math.ceil(totalUsers / usersPerPage);
  const paginationInfo = document.getElementById('paginationInfo');
  const paginationPages = document.getElementById('paginationPages');
  const prevBtn = document.getElementById('prevPage');
  const nextBtn = document.getElementById('nextPage');
  
  console.log('📄 Rendering pagination:', { totalUsers, totalPages, currentPage });
  
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
  const totalPages = Math.ceil((window.totalUsersCount || filteredUsers.length) / usersPerPage);
  console.log('📄 Changing to page:', page, 'of', totalPages);
  if (page >= 1 && page <= totalPages) {
    currentPage = page;
    
    // Get current search query
    const searchInput = document.getElementById('userSearch');
    const searchQuery = searchInput ? searchInput.value.trim() : '';
    
    // Load new page from API
    loadUsers(page, searchQuery);
    console.log('✅ Page changed successfully');
  } else {
    console.log('❌ Invalid page number');
  }
}

// Logout function
function logout() {
  localStorage.removeItem('token');
  localStorage.removeItem('user');
  window.location.href = '/';
}
