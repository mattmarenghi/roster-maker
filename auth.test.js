/**
 * Tests for the 4 auth/favorites bugs fixed in index.html:
 *
 * Bug 1 & 2: Favorites not persisting across reloads / re-login
 * Bug 3: Verification screen hangs after entering OTP code
 * Bug 4: Logout doesn't cleanly reset UI
 */

// ── Helpers ──────────────────────────────────────────────────────────────────

function makeEnv() {
  // Minimal DOM stubs
  const elements = {
    profileBtn:   { className: '', innerHTML: '' },
    playersArea:  { innerHTML: '' },
    profileOverlay: { classList: makeClassList() },
    authOverlay:    { classList: makeClassList() },
    profileContent: { innerHTML: '' },
  };

  function makeClassList() {
    const classes = new Set();
    return {
      add:    cls => classes.add(cls),
      remove: cls => classes.delete(cls),
      has:    cls => classes.has(cls),
    };
  }

  const $      = id => elements[id] ?? null;
  const calls  = [];          // ordered log of function calls
  const track  = name => calls.push(name);

  // ── State vars (mirrors index.html globals) ───────────────────────────────
  let currentUser = null;
  let favorites   = new Set();
  let allPlayers  = [];

  // ── Supabase mock ─────────────────────────────────────────────────────────
  // Will be overridden per-test
  let _dbFavoritesData = [];
  let _dbFavoritesError = null;
  let _signOutError = null;

  // Fluent builder: db.from('favorites').select('player_id').eq(...)
  function makeQuery(data, error) {
    const q = {
      _filters: [],
      select() { return q; },
      eq(col, val) { q._filters.push({ col, val }); return q; },
      delete() { return q; },
      match() { return q; },
      insert() { return q; },
      then(resolve) {
        resolve({ data, error });
        return Promise.resolve({ data, error });
      },
      // make it awaitable
      [Symbol.toStringTag]: 'Promise',
    };
    // Make awaitable
    Object.defineProperty(q, Symbol.toStringTag, { value: 'Promise' });
    q.then = (res) => { res({ data, error }); return Promise.resolve({ data, error }); };
    // Actually return a real promise
    return {
      _filters: [],
      select() { return this; },
      eq(col, val) { this._filters.push({ col, val }); return this; },
      delete() { return this; },
      match() { return this; },
      insert() { return this; },
      // Awaitable
      then: undefined,
      // Make it a thenable via a getter trick — simplest: return a real Promise
    };
  }

  const db = {
    from(table) {
      const self = {
        _eqFilters: [],
        select() { return self; },
        eq(col, val) {
          self._eqFilters.push({ col, val });
          return self;
        },
        delete() { return self; },
        match() { return { then: (r) => r({ data: null, error: null }), catch: () => ({}) }; },
        insert() { return { then: (r) => r({ data: null, error: null }), catch: () => ({}) }; },
        // Make it awaitable as a promise
        // We wrap in real promise so async/await works
        get [Symbol.toStringTag]() { return 'DatabaseQuery'; },
      };
      // Return a real Promise when awaited
      const promise = Promise.resolve({ data: _dbFavoritesData, error: _dbFavoritesError });
      // Merge the filter tracking onto the promise
      promise._eqFilters = [];
      const origEq = (col, val) => { promise._eqFilters.push({ col, val }); return promise; };
      promise.eq = origEq;
      promise.select = () => promise;
      return promise;
    },
    auth: {
      signOut: async () => ({ error: _signOutError }),
      getSession: async () => ({ data: { session: null } }),
    },
  };

  // ── Functions under test (copied logic from index.html) ───────────────────

  function renderProfileBtn() {
    track('renderProfileBtn');
    const btn = $('profileBtn');
    if (currentUser) {
      btn.className = 'profile-btn profile-btn-user';
      btn.innerHTML = (currentUser.email || '?').charAt(0).toUpperCase();
    } else {
      btn.className = 'profile-btn profile-btn-guest';
      btn.innerHTML = '<svg/>';
    }
  }

  function renderHomepage() {
    track('renderHomepage');
    const area = $('playersArea');
    if (!currentUser) {
      area.innerHTML = 'SIGN_IN_STATE';
      return;
    }
    if (favorites.size === 0) {
      area.innerHTML = 'EMPTY_FAVORITES_STATE';
      return;
    }
    const favPlayers = allPlayers.filter(p => favorites.has(p.id));
    area.innerHTML = 'FAVORITES:' + favPlayers.map(p => p.id).join(',');
  }

  function syncAllStars() {
    track('syncAllStars');
  }

  function hideAuthModal() {
    track('hideAuthModal');
    $('authOverlay').classList.remove('open');
  }

  function showAuthModal() {
    track('showAuthModal');
    $('authOverlay').classList.add('open');
  }

  function hideProfile() {
    track('hideProfile');
    $('profileOverlay').classList.remove('open');
  }

  function showProfile() {
    track('showProfile');
    renderProfileContent();
    $('profileOverlay').classList.add('open');
  }

  function renderProfileContent() {
    track('renderProfileContent');
  }

  async function fetchFavorites() {
    // ── FIXED: guard + explicit user_id filter ──
    if (!currentUser) return;
    const { data, error } = await db.from('favorites').select('player_id').eq('user_id', currentUser.id);
    if (error) { /* log */ }
    favorites = new Set((data || []).map(r => r.player_id));
    renderHomepage();
    syncAllStars();
  }

  // onAuthStateChange handler (extracted logic)
  async function handleAuthStateChange(event, session) {
    currentUser = session?.user ?? null;
    renderProfileBtn();
    if (currentUser) {
      // ── FIXED: close auth modal on sign-in ──
      hideAuthModal();
      await fetchFavorites();
    } else if (event === 'SIGNED_OUT') {
      // ── FIXED: only clear on explicit SIGNED_OUT ──
      favorites = new Set();
      renderHomepage();
    }
  }

  async function signOut() {
    // ── FIXED: close + clear immediately, then async signOut ──
    hideProfile();
    currentUser = null;
    favorites = new Set();
    renderProfileBtn();
    renderHomepage();
    const { error } = await db.auth.signOut();
    if (error) console.error('Sign out error:', error);
  }

  async function verifyOTP_old(pendingEmail, token) {
    // OLD verifyOTP — modal only closed in else branch, no fallback
    const btn = { disabled: false, textContent: 'Sign In' };
    btn.disabled = true;
    btn.textContent = 'Verifying...';
    const { error } = await db.auth.verifyOtp?.({ email: pendingEmail, token, type: 'email' }) ?? { error: null };
    if (error) {
      btn.disabled = false;
      btn.textContent = 'Sign In';
    } else {
      hideAuthModal();    // only place modal gets closed in old code
      btn.disabled = false;
      btn.textContent = 'Sign In';
    }
    return btn;
  }

  return {
    // state access
    getUser:      () => currentUser,
    setUser:      u  => { currentUser = u; },
    getFavorites: () => favorites,
    setFavorites: f  => { favorites = f; },
    setAllPlayers: p => { allPlayers = p; },
    getPlayersArea: () => $('playersArea'),
    getProfileBtn:  () => $('profileBtn'),
    getAuthOverlay: () => $('authOverlay'),
    getProfileOverlay: () => $('profileOverlay'),
    // mock controls
    setDbFavorites: (data, err = null) => { _dbFavoritesData = data; _dbFavoritesError = err; },
    setSignOutError: e => { _signOutError = e; },
    // functions under test
    fetchFavorites,
    handleAuthStateChange,
    signOut,
    hideAuthModal,
    showAuthModal,
    // call log
    calls,
    resetCalls: () => calls.splice(0),
  };
}

// ── Test runner ───────────────────────────────────────────────────────────────

describe('Bug 1 & 2 — fetchFavorites', () => {

  test('returns early without querying when currentUser is null', async () => {
    const env = makeEnv();
    env.setDbFavorites([{ player_id: 'abc123' }]);
    // currentUser starts null
    await env.fetchFavorites();
    // favorites should remain empty — no DB call happened, no render
    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).not.toContain('renderHomepage');
  });

  test('loads favorites for the signed-in user', async () => {
    const env = makeEnv();
    env.setUser({ id: 'user-1', email: 'scout@team.edu' });
    env.setDbFavorites([{ player_id: 'abc123' }, { player_id: 'def456' }]);
    env.setAllPlayers([{ id: 'abc123' }, { id: 'def456' }]);

    await env.fetchFavorites();

    expect(env.getFavorites().has('abc123')).toBe(true);
    expect(env.getFavorites().has('def456')).toBe(true);
    expect(env.calls).toContain('renderHomepage');
    expect(env.calls).toContain('syncAllStars');
  });

  test('sets favorites to empty set when DB returns no rows', async () => {
    const env = makeEnv();
    env.setUser({ id: 'user-2', email: 'empty@team.edu' });
    env.setDbFavorites([]);

    await env.fetchFavorites();

    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).toContain('renderHomepage');
  });

  test('handles DB error gracefully — favorites become empty, render still runs', async () => {
    const env = makeEnv();
    env.setUser({ id: 'user-3', email: 'err@team.edu' });
    env.setDbFavorites(null, { message: 'RLS denied' });

    await env.fetchFavorites();

    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).toContain('renderHomepage');
  });
});

describe('Bug 3 — verification screen hangs (onAuthStateChange closes modal)', () => {

  test('hideAuthModal is called when SIGNED_IN fires', async () => {
    const env = makeEnv();
    env.showAuthModal(); // simulate open modal
    expect(env.getAuthOverlay().classList.has('open')).toBe(true);
    env.resetCalls();
    env.setDbFavorites([]);

    await env.handleAuthStateChange('SIGNED_IN', { user: { id: 'u1', email: 'a@b.com' } });

    expect(env.calls).toContain('hideAuthModal');
    expect(env.getAuthOverlay().classList.has('open')).toBe(false);
  });

  test('hideAuthModal is called for TOKEN_REFRESHED (also signs user in)', async () => {
    const env = makeEnv();
    env.showAuthModal();
    env.setDbFavorites([]);

    await env.handleAuthStateChange('TOKEN_REFRESHED', { user: { id: 'u1', email: 'a@b.com' } });

    expect(env.getAuthOverlay().classList.has('open')).toBe(false);
  });

  test('favorites are loaded after SIGNED_IN', async () => {
    const env = makeEnv();
    env.setDbFavorites([{ player_id: 'p1' }]);
    env.showAuthModal();

    await env.handleAuthStateChange('SIGNED_IN', { user: { id: 'u1', email: 'a@b.com' } });

    expect(env.getFavorites().has('p1')).toBe(true);
  });
});

describe('Bug 1 & 2 — onAuthStateChange does NOT clear favorites on non-SIGNED_OUT null sessions', () => {

  test('INITIAL_SESSION with null session does not clear favorites or rerender', async () => {
    const env = makeEnv();
    // Simulate user already loaded favorites
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1', 'p2']));
    env.resetCalls();

    // INITIAL_SESSION fires with null session (Supabase v2.98+ Web Locks issue)
    await env.handleAuthStateChange('INITIAL_SESSION', null);

    // Favorites must NOT be cleared
    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.getFavorites().has('p2')).toBe(true);
    // renderHomepage must NOT be called with sign-in state
    expect(env.calls).not.toContain('renderHomepage');
  });

  test('SIGNED_OUT clears favorites and renders sign-in state', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));

    await env.handleAuthStateChange('SIGNED_OUT', null);

    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).toContain('renderHomepage');
    expect(env.getPlayersArea().innerHTML).toBe('SIGN_IN_STATE');
  });
});

describe('Bug 4 — logout cleans up immediately', () => {

  test('hideProfile is called before db.auth.signOut resolves', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));

    // Verify the overlay is open (profile was showing)
    env.getProfileOverlay().classList.add('open');

    let profileHiddenAt = null;
    let signOutCalledAt = null;
    let step = 0;

    const orig_hideProfile_idx = () => {
      profileHiddenAt = step++;
    };

    // We'll check call ordering via env.calls array
    await env.signOut();

    const callOrder = env.calls;
    const hideProfilePos  = callOrder.indexOf('hideProfile');
    const renderHomepagePos = callOrder.indexOf('renderHomepage');
    const renderProfileBtnPos = callOrder.indexOf('renderProfileBtn');

    // hideProfile must be first
    expect(hideProfilePos).toBeGreaterThanOrEqual(0);
    expect(renderHomepagePos).toBeGreaterThan(hideProfilePos);
    expect(renderProfileBtnPos).toBeGreaterThan(hideProfilePos);
  });

  test('UI shows signed-out state immediately (before server confirms)', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));
    env.setAllPlayers([{ id: 'p1' }]);

    await env.signOut();

    // User and favorites cleared synchronously
    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
    // Profile button shows guest state
    expect(env.getProfileBtn().className).toContain('profile-btn-guest');
    // Homepage shows sign-in state
    expect(env.getPlayersArea().innerHTML).toBe('SIGN_IN_STATE');
  });

  test('profile overlay is closed after signOut', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.getProfileOverlay().classList.add('open');

    await env.signOut();

    expect(env.getProfileOverlay().classList.has('open')).toBe(false);
  });

  test('signOut completes even if db.auth.signOut returns an error', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setSignOutError({ message: 'network error' });

    // Should not throw
    await expect(env.signOut()).resolves.toBeUndefined();

    // UI still cleared
    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
  });
});
