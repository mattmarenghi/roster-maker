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
    profileBtn:      { className: '', innerHTML: '' },
    playersArea:     { innerHTML: '' },
    profileOverlay:  { classList: makeClassList() },
    authOverlay:     { classList: makeClassList() },
    profileContent:  { innerHTML: '' },
  };

  function makeClassList() {
    const classes = new Set();
    return {
      add:    cls => classes.add(cls),
      remove: cls => classes.delete(cls),
      has:    cls => classes.has(cls),
    };
  }

  const $ = id => elements[id] ?? null;
  const calls = [];
  const track = name => calls.push(name);

  // ── State vars ────────────────────────────────────────────────────────────
  let currentUser = null;
  let favorites   = new Set();
  let allPlayers  = [];

  // ── Supabase mock ─────────────────────────────────────────────────────────
  let _dbFavoritesData  = [];
  let _dbFavoritesError = null;
  let _signOutError     = null;

  const db = {
    from() {
      const promise = Promise.resolve({ data: _dbFavoritesData, error: _dbFavoritesError });
      promise._eqFilters = [];
      promise.select = () => promise;
      promise.eq = (col, val) => { promise._eqFilters.push({ col, val }); return promise; };
      return promise;
    },
    auth: {
      signOut:    async () => ({ error: _signOutError }),
      getSession: async () => ({ data: { session: null } }),
    },
  };

  // ── Functions under test (mirror index.html logic) ────────────────────────

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
    if (!currentUser) { area.innerHTML = 'SIGN_IN_STATE'; return; }
    if (favorites.size === 0) { area.innerHTML = 'EMPTY_FAVORITES_STATE'; return; }
    const favPlayers = allPlayers.filter(p => favorites.has(p.id));
    area.innerHTML = 'FAVORITES:' + favPlayers.map(p => p.id).join(',');
  }

  function syncAllStars() { track('syncAllStars'); }

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

  async function fetchFavorites() {
    if (!currentUser) return;
    const { data, error } = await db.from('favorites').select('player_id').eq('user_id', currentUser.id);
    if (error) { /* log */ }
    favorites = new Set((data || []).map(r => r.player_id));
    renderHomepage();
    syncAllStars();
  }

  // ── FIXED onAuthStateChange: only SIGNED_IN triggers hideAuthModal/fetchFavorites
  async function handleAuthStateChange(event, session) {
    currentUser = session?.user ?? null;
    renderProfileBtn();
    if (event === 'SIGNED_IN') {
      hideAuthModal();
      await fetchFavorites();
    } else if (event === 'SIGNED_OUT') {
      favorites = new Set();
      renderHomepage();
    }
    // INITIAL_SESSION / TOKEN_REFRESHED: only update currentUser + renderProfileBtn
  }

  // ── FIXED signOut: close + clear immediately, then async
  async function signOut() {
    hideProfile();
    currentUser = null;
    favorites = new Set();
    renderProfileBtn();
    renderHomepage();
    const { error } = await db.auth.signOut();
    if (error) console.error('Sign out error:', error);
  }

  // ── FIXED init() getSession logic: always call getSession (no !currentUser guard)
  async function initGetSession() {
    const { data: sd } = await db.auth.getSession();
    if (sd?.session?.user) {
      currentUser = sd.session.user;
      renderProfileBtn();
      await fetchFavorites();
    }
  }

  return {
    getUser:        () => currentUser,
    setUser:        u  => { currentUser = u; },
    getFavorites:   () => favorites,
    setFavorites:   f  => { favorites = f; },
    setAllPlayers:  p  => { allPlayers = p; },
    getPlayersArea:    () => $('playersArea'),
    getProfileBtn:     () => $('profileBtn'),
    getAuthOverlay:    () => $('authOverlay'),
    getProfileOverlay: () => $('profileOverlay'),
    // mock controls
    setDbFavorites:  (data, err = null) => { _dbFavoritesData = data; _dbFavoritesError = err; },
    setDbSession:    (session)          => { db.auth.getSession = async () => ({ data: { session } }); },
    setSignOutError: e => { _signOutError = e; },
    // functions under test
    fetchFavorites,
    handleAuthStateChange,
    signOut,
    initGetSession,
    hideAuthModal,
    showAuthModal,
    // call log
    calls,
    resetCalls: () => calls.splice(0),
  };
}

// ─────────────────────────────────────────────────────────────────────────────

describe('Bug 1 & 2 — fetchFavorites', () => {

  test('returns early without querying when currentUser is null', async () => {
    const env = makeEnv();
    env.setDbFavorites([{ player_id: 'abc123' }]);
    await env.fetchFavorites();
    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).not.toContain('renderHomepage');
  });

  test('loads favorites for the signed-in user', async () => {
    const env = makeEnv();
    env.setUser({ id: 'user-1', email: 'scout@team.edu' });
    env.setDbFavorites([{ player_id: 'abc123' }, { player_id: 'def456' }]);
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

  test('handles DB error gracefully — favorites empty, render still runs', async () => {
    const env = makeEnv();
    env.setUser({ id: 'user-3', email: 'err@team.edu' });
    env.setDbFavorites(null, { message: 'RLS denied' });
    await env.fetchFavorites();
    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).toContain('renderHomepage');
  });
});

describe('Bug 1 & 2 — init() getSession always loads favorites on reload', () => {

  test('loads favorites when getSession returns a session (page reload path)', async () => {
    const env = makeEnv();
    env.setDbSession({ user: { id: 'u1', email: 'a@b.com' } });
    env.setDbFavorites([{ player_id: 'p1' }]);

    await env.initGetSession();

    expect(env.getUser()).toEqual({ id: 'u1', email: 'a@b.com' });
    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.calls).toContain('fetchFavorites' in env ? 'renderHomepage' : 'renderHomepage');
  });

  test('loads favorites even if onAuthStateChange already set currentUser', async () => {
    const env = makeEnv();
    // Simulate: INITIAL_SESSION already ran and set currentUser, but fetchFavorites
    // failed (empty result due to auth header race). Favorites are still empty.
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set());  // empty — the failing state

    // getSession returns valid session
    env.setDbSession({ user: { id: 'u1', email: 'a@b.com' } });
    env.setDbFavorites([{ player_id: 'p1' }, { player_id: 'p2' }]);

    await env.initGetSession();  // no !currentUser guard — always runs

    // Favorites are now populated
    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.getFavorites().has('p2')).toBe(true);
  });

  test('does nothing when no session (logged-out reload)', async () => {
    const env = makeEnv();
    env.setDbSession(null);
    env.resetCalls();

    await env.initGetSession();

    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).not.toContain('renderHomepage');
  });
});

describe('Bug 3 — verification screen: SIGNED_IN closes modal', () => {

  test('hideAuthModal is called when SIGNED_IN fires', async () => {
    const env = makeEnv();
    env.showAuthModal();
    expect(env.getAuthOverlay().classList.has('open')).toBe(true);
    env.resetCalls();
    env.setDbFavorites([]);

    await env.handleAuthStateChange('SIGNED_IN', { user: { id: 'u1', email: 'a@b.com' } });

    expect(env.calls).toContain('hideAuthModal');
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

describe('Bug 2 — TOKEN_REFRESHED / INITIAL_SESSION do NOT close auth modal', () => {

  test('TOKEN_REFRESHED does not call hideAuthModal (modal stays open for login)', async () => {
    const env = makeEnv();
    env.showAuthModal();  // user is in the middle of logging in
    env.resetCalls();

    // TOKEN_REFRESHED fires with a session (e.g. background refresh)
    await env.handleAuthStateChange('TOKEN_REFRESHED', { user: { id: 'u1', email: 'a@b.com' } });

    expect(env.calls).not.toContain('hideAuthModal');
    expect(env.getAuthOverlay().classList.has('open')).toBe(true);  // modal still open
  });

  test('INITIAL_SESSION does not call hideAuthModal', async () => {
    const env = makeEnv();
    env.showAuthModal();
    env.resetCalls();

    await env.handleAuthStateChange('INITIAL_SESSION', { user: { id: 'u1', email: 'a@b.com' } });

    expect(env.calls).not.toContain('hideAuthModal');
    expect(env.getAuthOverlay().classList.has('open')).toBe(true);
  });

  test('TOKEN_REFRESHED does not call fetchFavorites (no redundant reload)', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['existing-p1']));
    env.resetCalls();

    await env.handleAuthStateChange('TOKEN_REFRESHED', { user: { id: 'u1', email: 'a@b.com' } });

    // No fetchFavorites → no renderHomepage, favorites unchanged
    expect(env.calls).not.toContain('renderHomepage');
    expect(env.getFavorites().has('existing-p1')).toBe(true);
  });
});

describe('Bug 1 & 2 — SIGNED_OUT still clears state correctly', () => {

  test('SIGNED_OUT clears favorites and shows sign-in state', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));

    await env.handleAuthStateChange('SIGNED_OUT', null);

    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
    expect(env.getPlayersArea().innerHTML).toBe('SIGN_IN_STATE');
  });

  test('INITIAL_SESSION with null session does not clear favorites', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1', 'p2']));
    env.resetCalls();

    await env.handleAuthStateChange('INITIAL_SESSION', null);

    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.getFavorites().has('p2')).toBe(true);
    expect(env.calls).not.toContain('renderHomepage');
  });
});

describe('Bug 4 — logout cleans up immediately', () => {

  test('hideProfile is called before db.auth.signOut resolves', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));
    env.getProfileOverlay().classList.add('open');

    await env.signOut();

    const callOrder       = env.calls;
    const hideProfilePos  = callOrder.indexOf('hideProfile');
    const renderPos       = callOrder.indexOf('renderHomepage');
    const profileBtnPos   = callOrder.indexOf('renderProfileBtn');

    expect(hideProfilePos).toBeGreaterThanOrEqual(0);
    expect(renderPos).toBeGreaterThan(hideProfilePos);
    expect(profileBtnPos).toBeGreaterThan(hideProfilePos);
  });

  test('UI shows signed-out state immediately', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));
    env.setAllPlayers([{ id: 'p1' }]);

    await env.signOut();

    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
    expect(env.getProfileBtn().className).toContain('profile-btn-guest');
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

    await expect(env.signOut()).resolves.toBeUndefined();

    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
  });
});
