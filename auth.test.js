/**
 * Tests for auth/favorites fixes in index.html.
 *
 * Architecture under test:
 *   - onAuthStateChange: state sync only (no async data loading)
 *   - verifyOTP: owns the login flow — sets currentUser from response data,
 *                closes modal, loads favorites, resets button
 *   - init() getSession: canonical page-load favorites restoration
 *   - signOut: clears state immediately before async signOut call
 *   - toggleFavorite: optimistic update with DB persist; rollback + visible
 *                     error toast on failure; captures user at call time to
 *                     avoid race with concurrent signOut
 */

// ── Test environment factory ──────────────────────────────────────────────────

function makeEnv() {
  function makeClassList() {
    const classes = new Set();
    return { add: c => classes.add(c), remove: c => classes.delete(c), has: c => classes.has(c) };
  }

  const elements = {
    profileBtn:      { className: '', innerHTML: '' },
    playersArea:     { innerHTML: '' },
    profileOverlay:  { classList: makeClassList() },
    authOverlay:     { classList: makeClassList() },
  };

  const $ = id => elements[id] ?? null;
  const calls = [];
  const track = name => calls.push(name);

  // ── State ─────────────────────────────────────────────────────────────────
  let currentUser = null;
  let favorites   = new Set();
  let allPlayers  = [];

  // ── Supabase mock ─────────────────────────────────────────────────────────
  let _favData      = [];
  let _favError     = null;
  let _deleteError  = null;
  let _insertError  = null;
  let _signOutError = null;
  let _sessionUser  = null;
  let _verifyUser   = null;   // user returned in data.user from verifyOtp
  let _verifyError  = null;
  let _verifyThrows = false;

  const db = {
    from() {
      // select chain: .select().eq() → resolves with favData
      const selectPromise = Promise.resolve({ data: _favData, error: _favError });
      selectPromise.select = () => selectPromise;
      selectPromise.eq     = () => selectPromise;

      // delete chain: .delete().match() → resolves with deleteError
      const deleteChain = {
        match: () => Promise.resolve({ data: null, error: _deleteError }),
      };

      // insert: .insert() → resolves with insertError
      const insertResult = Promise.resolve({ data: null, error: _insertError });

      return {
        select: () => selectPromise,
        delete: () => deleteChain,
        insert: () => insertResult,
      };
    },
    auth: {
      signOut:    async () => ({ error: _signOutError }),
      getSession: async () => ({ data: { session: _sessionUser ? { user: _sessionUser } : null } }),
      verifyOtp:  async () => {
        if (_verifyThrows) throw new Error('network failure');
        return { data: { user: _verifyUser }, error: _verifyError };
      },
    },
  };

  // ── Functions mirroring index.html ────────────────────────────────────────

  function renderProfileBtn() {
    track('renderProfileBtn');
    const btn = $('profileBtn');
    if (currentUser) {
      btn.className = 'profile-btn profile-btn-user';
      btn.innerHTML = (currentUser.email || '?')[0].toUpperCase();
    } else {
      btn.className = 'profile-btn profile-btn-guest';
      btn.innerHTML = '<svg/>';
    }
  }

  function renderHomepage() {
    track('renderHomepage');
    const area = $('playersArea');
    if (!currentUser)         { area.innerHTML = 'SIGN_IN_STATE'; return; }
    if (favorites.size === 0) { area.innerHTML = 'EMPTY_STATE';   return; }
    area.innerHTML = 'FAVORITES:' + allPlayers.filter(p => favorites.has(p.id)).map(p => p.id).join(',');
  }

  function syncAllStars() { track('syncAllStars'); }
  function hideAuthModal() { track('hideAuthModal'); $('authOverlay').classList.remove('open'); }
  function showAuthModal() { track('showAuthModal'); $('authOverlay').classList.add('open'); }
  function hideProfile()   { track('hideProfile');   $('profileOverlay').classList.remove('open'); }

  function showFavToast(removing)      { track(removing ? 'showFavToast:remove' : 'showFavToast:add'); }
  function showFavErrorToast(removing) { track(removing ? 'showFavErrorToast:remove' : 'showFavErrorToast:add'); }

  async function fetchFavorites() {
    if (!currentUser) return;
    const { data, error } = await db.from('favorites').select('player_id').eq('user_id', currentUser.id);
    if (error) { /* log */ }
    favorites = new Set((data || []).map(r => r.player_id));
    renderHomepage();
    syncAllStars();
  }

  // onAuthStateChange — state sync only, no async data loading
  function handleAuthStateChange(event, session) {
    currentUser = session?.user ?? null;
    renderProfileBtn();
    if (event === 'SIGNED_OUT') {
      favorites = new Set();
      renderHomepage();
    }
  }

  // verifyOTP — owns the complete login flow.
  // Sets currentUser from response data so fetchFavorites() always has a user,
  // regardless of whether onAuthStateChange(SIGNED_IN) has fired yet.
  const authBtn = { disabled: false, textContent: 'Sign In' };
  async function verifyOTP(token) {
    if (!token || token.length !== 6) return;
    authBtn.disabled = true;
    authBtn.textContent = 'Verifying...';
    try {
      const { data, error } = await db.auth.verifyOtp({ token });
      if (error) {
        // show error (omit DOM detail in tests)
      } else {
        if (data?.user) {
          currentUser = data.user;
          renderProfileBtn();
        }
        hideAuthModal();
        await fetchFavorites();
      }
    } catch (err) {
      // show generic error message
    } finally {
      authBtn.disabled = false;
      authBtn.textContent = 'Sign In';
    }
  }

  // init() getSession block — page-load favorites restoration
  async function initGetSession() {
    const { data: sd } = await db.auth.getSession();
    if (sd?.session?.user) {
      currentUser = sd.session.user;
      renderProfileBtn();
      await fetchFavorites();
    }
  }

  // signOut — clear immediately, async signOut after
  async function signOut() {
    hideProfile();
    currentUser = null;
    favorites = new Set();
    renderProfileBtn();
    renderHomepage();
    const { error } = await db.auth.signOut();
    if (error) console.error('Sign out error:', error);
  }

  // toggleFavorite — optimistic update + DB persist; rollback + error toast
  // on failure; captures currentUser at call time to avoid race with signOut.
  async function toggleFavorite(id) {
    if (!currentUser) {
      showAuthModal();
      return;
    }
    const user     = currentUser; // capture before any async gap
    const removing = favorites.has(id);

    if (removing) { favorites.delete(id); } else { favorites.add(id); }
    syncAllStars();
    showFavToast(removing);
    renderHomepage();

    if (removing) {
      const { error } = await db.from('favorites').delete().match({ user_id: user.id, player_id: id });
      if (error) {
        console.error('Failed to remove favorite:', error);
        showFavErrorToast(true);
        favorites.add(id);
        syncAllStars();
        renderHomepage();
      }
    } else {
      const { error } = await db.from('favorites').insert({ user_id: user.id, player_id: id });
      if (error) {
        console.error('Failed to save favorite:', error);
        showFavErrorToast(false);
        favorites.delete(id);
        syncAllStars();
        renderHomepage();
      }
    }
  }

  return {
    getUser:           () => currentUser,
    setUser:           u  => { currentUser = u; },
    getFavorites:      () => favorites,
    setFavorites:      f  => { favorites = f; },
    setAllPlayers:     p  => { allPlayers = p; },
    getPlayersArea:    () => $('playersArea'),
    getProfileBtn:     () => $('profileBtn'),
    getAuthOverlay:    () => $('authOverlay'),
    getProfileOverlay: () => $('profileOverlay'),
    getAuthBtn:        () => authBtn,
    // mock controls
    setFavData:        (data, err = null) => { _favData = data; _favError = err; },
    setSessionUser:    u  => { _sessionUser = u; },
    setSignOutError:   e  => { _signOutError = e; },
    setVerifyUser:     u  => { _verifyUser = u; },
    setVerifyError:    e  => { _verifyError = e; },
    setVerifyThrows:   () => { _verifyThrows = true; },
    setDeleteError:    e  => { _deleteError = e; },
    setInsertError:    e  => { _insertError = e; },
    // functions under test
    fetchFavorites,
    handleAuthStateChange,
    verifyOTP,
    initGetSession,
    signOut,
    toggleFavorite,
    hideAuthModal,
    showAuthModal,
    calls,
    resetCalls: () => calls.splice(0),
  };
}

// ── fetchFavorites ────────────────────────────────────────────────────────────

describe('fetchFavorites', () => {
  test('returns early when currentUser is null — no query, no render', async () => {
    const env = makeEnv();
    env.setFavData([{ player_id: 'p1' }]);
    await env.fetchFavorites();
    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).not.toContain('renderHomepage');
  });

  test('loads favorites for signed-in user', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavData([{ player_id: 'p1' }, { player_id: 'p2' }]);
    await env.fetchFavorites();
    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.getFavorites().has('p2')).toBe(true);
    expect(env.calls).toContain('renderHomepage');
  });

  test('handles empty result', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavData([]);
    await env.fetchFavorites();
    expect(env.getFavorites().size).toBe(0);
  });

  test('handles DB error gracefully', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavData(null, { message: 'RLS denied' });
    await env.fetchFavorites();
    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).toContain('renderHomepage');
  });
});

// ── onAuthStateChange ─────────────────────────────────────────────────────────

describe('onAuthStateChange — state sync only', () => {
  test('SIGNED_IN updates currentUser and profile button, does NOT load favorites or close modal', async () => {
    const env = makeEnv();
    env.showAuthModal();
    env.resetCalls();

    env.handleAuthStateChange('SIGNED_IN', { user: { id: 'u1', email: 'a@b.com' } });

    expect(env.getUser()).toEqual({ id: 'u1', email: 'a@b.com' });
    expect(env.calls).toContain('renderProfileBtn');
    expect(env.calls).not.toContain('hideAuthModal');   // verifyOTP owns modal close
    expect(env.calls).not.toContain('renderHomepage');  // no favorites load here
    expect(env.getAuthOverlay().classList.has('open')).toBe(true); // modal still open
  });

  test('SIGNED_OUT clears favorites and renders sign-in state', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));

    env.handleAuthStateChange('SIGNED_OUT', null);

    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
    expect(env.getPlayersArea().innerHTML).toBe('SIGN_IN_STATE');
  });

  test('TOKEN_REFRESHED updates state but does not load favorites or close modal', async () => {
    const env = makeEnv();
    env.showAuthModal();
    env.setFavorites(new Set(['existing']));
    env.resetCalls();

    env.handleAuthStateChange('TOKEN_REFRESHED', { user: { id: 'u1', email: 'a@b.com' } });

    expect(env.calls).not.toContain('hideAuthModal');
    expect(env.calls).not.toContain('renderHomepage');
    expect(env.getFavorites().has('existing')).toBe(true); // unchanged
    expect(env.getAuthOverlay().classList.has('open')).toBe(true); // modal still open
  });

  test('INITIAL_SESSION with null session does not clear favorites', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1', 'p2']));

    env.handleAuthStateChange('INITIAL_SESSION', null);

    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.getFavorites().has('p2')).toBe(true);
  });
});

// ── verifyOTP ─────────────────────────────────────────────────────────────────

describe('verifyOTP — owns the login flow', () => {
  test('success: sets currentUser from response data, closes modal, loads favorites, resets button', async () => {
    const env = makeEnv();
    // currentUser is null — simulates onAuthStateChange(SIGNED_IN) not yet fired.
    // verifyOTP must set currentUser itself from data.user in the response.
    env.setVerifyUser({ id: 'u1', email: 'a@b.com' });
    env.showAuthModal();
    env.setFavData([{ player_id: 'p1' }]);

    await env.verifyOTP('123456');

    expect(env.getUser()).toEqual({ id: 'u1', email: 'a@b.com' });
    expect(env.calls).toContain('renderProfileBtn');
    expect(env.calls).toContain('hideAuthModal');
    expect(env.getAuthOverlay().classList.has('open')).toBe(false);
    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.getAuthBtn().disabled).toBe(false);
    expect(env.getAuthBtn().textContent).toBe('Sign In');
  });

  test('success: fetchFavorites works even when onAuthStateChange has not yet fired (currentUser was null)', async () => {
    const env = makeEnv();
    // Explicitly leave currentUser as null before verifyOTP — this is the race
    // condition that used to cause fetchFavorites() to bail early.
    expect(env.getUser()).toBeNull();

    env.setVerifyUser({ id: 'u2', email: 'scout@team.com' });
    env.setFavData([{ player_id: 'abc' }, { player_id: 'def' }]);
    env.showAuthModal();

    await env.verifyOTP('654321');

    // currentUser is now set from the response, not from onAuthStateChange
    expect(env.getUser()).toEqual({ id: 'u2', email: 'scout@team.com' });
    // Favorites were loaded (fetchFavorites did not bail)
    expect(env.getFavorites().has('abc')).toBe(true);
    expect(env.getFavorites().has('def')).toBe(true);
  });

  test('success: no-op when data.user is missing from response (defensive)', async () => {
    const env = makeEnv();
    env.setVerifyUser(null); // response has data: { user: null }
    env.setFavData([]);
    env.showAuthModal();

    // Should not throw; fetchFavorites bails early (currentUser still null)
    await expect(env.verifyOTP('123456')).resolves.toBeUndefined();
    expect(env.getUser()).toBeNull();
  });

  test('error: shows error, resets button, modal stays open', async () => {
    const env = makeEnv();
    env.showAuthModal();
    env.setVerifyError({ message: 'Invalid OTP' });

    await env.verifyOTP('000000');

    expect(env.calls).not.toContain('hideAuthModal');
    expect(env.getAuthOverlay().classList.has('open')).toBe(true);
    expect(env.getFavorites().size).toBe(0);
    expect(env.getAuthBtn().disabled).toBe(false);
    expect(env.getAuthBtn().textContent).toBe('Sign In');
  });

  test('network throw: button always resets (never stuck on Verifying…)', async () => {
    const env = makeEnv();
    env.showAuthModal();
    env.setVerifyThrows();

    await env.verifyOTP('123456');

    expect(env.getAuthBtn().disabled).toBe(false);
    expect(env.getAuthBtn().textContent).toBe('Sign In');
  });

  test('skips if token is not 6 digits', async () => {
    const env = makeEnv();
    env.showAuthModal();

    await env.verifyOTP('123');

    expect(env.calls).not.toContain('hideAuthModal');
  });
});

// ── init() getSession ─────────────────────────────────────────────────────────

describe('init() getSession — page-load favorites restoration', () => {
  test('loads favorites when a session exists', async () => {
    const env = makeEnv();
    env.setSessionUser({ id: 'u1', email: 'a@b.com' });
    env.setFavData([{ player_id: 'p1' }]);

    await env.initGetSession();

    expect(env.getUser()).toEqual({ id: 'u1', email: 'a@b.com' });
    expect(env.getFavorites().has('p1')).toBe(true);
  });

  test('runs even if onAuthStateChange already set currentUser (no guard)', async () => {
    const env = makeEnv();
    // Simulate: INITIAL_SESSION set currentUser but favorites are empty (auth header race)
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set());

    env.setSessionUser({ id: 'u1', email: 'a@b.com' });
    env.setFavData([{ player_id: 'p1' }, { player_id: 'p2' }]);

    await env.initGetSession(); // no !currentUser guard — always runs

    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.getFavorites().has('p2')).toBe(true);
  });

  test('does nothing when no session (logged-out page load)', async () => {
    const env = makeEnv();
    env.setSessionUser(null);
    env.resetCalls();

    await env.initGetSession();

    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
    expect(env.calls).not.toContain('renderHomepage');
  });
});

// ── signOut ───────────────────────────────────────────────────────────────────

describe('signOut', () => {
  test('closes profile and clears state before async signOut', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));
    env.getProfileOverlay().classList.add('open');

    await env.signOut();

    const hideIdx   = env.calls.indexOf('hideProfile');
    const renderIdx = env.calls.indexOf('renderHomepage');
    expect(hideIdx).toBeGreaterThanOrEqual(0);
    expect(renderIdx).toBeGreaterThan(hideIdx);
    expect(env.getUser()).toBeNull();
    expect(env.getFavorites().size).toBe(0);
    expect(env.getPlayersArea().innerHTML).toBe('SIGN_IN_STATE');
    expect(env.getProfileBtn().className).toContain('profile-btn-guest');
    expect(env.getProfileOverlay().classList.has('open')).toBe(false);
  });

  test('completes cleanly even if db.auth.signOut errors', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setSignOutError({ message: 'network error' });

    await expect(env.signOut()).resolves.toBeUndefined();
    expect(env.getUser()).toBeNull();
  });
});

// ── toggleFavorite ────────────────────────────────────────────────────────────

describe('toggleFavorite — optimistic update with DB persist', () => {
  test('adding: optimistic add + DB insert → favorite persists', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });

    await env.toggleFavorite('p1');

    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.calls).toContain('showFavToast:add');
    expect(env.calls).not.toContain('showFavErrorToast:add');
  });

  test('removing: optimistic remove + DB delete → favorite gone', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));

    await env.toggleFavorite('p1');

    expect(env.getFavorites().has('p1')).toBe(false);
    expect(env.calls).toContain('showFavToast:remove');
    expect(env.calls).not.toContain('showFavErrorToast:remove');
  });

  test('adding fails: rolls back optimistic add, shows error toast', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setInsertError({ message: 'RLS denied' });

    await env.toggleFavorite('p1');

    // Rolled back — should NOT be in favorites
    expect(env.getFavorites().has('p1')).toBe(false);
    expect(env.calls).toContain('showFavErrorToast:add');
    // The optimistic toast was shown, then the error toast was shown
    expect(env.calls).toContain('showFavToast:add');
  });

  test('removing fails: rolls back optimistic remove, shows error toast', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });
    env.setFavorites(new Set(['p1']));
    env.setDeleteError({ message: 'network error' });

    await env.toggleFavorite('p1');

    // Rolled back — should still be in favorites
    expect(env.getFavorites().has('p1')).toBe(true);
    expect(env.calls).toContain('showFavErrorToast:remove');
    expect(env.calls).toContain('showFavToast:remove');
  });

  test('unauthenticated: shows auth modal, no optimistic update', async () => {
    const env = makeEnv();
    // currentUser is null

    await env.toggleFavorite('p1');

    expect(env.calls).toContain('showAuthModal');
    expect(env.getFavorites().has('p1')).toBe(false);
  });

  test('captures currentUser at call time — survives concurrent signOut', async () => {
    const env = makeEnv();
    const user = { id: 'u1', email: 'a@b.com' };
    env.setUser(user);

    // Start toggleFavorite (async)
    const togglePromise = env.toggleFavorite('p1');

    // signOut fires concurrently, clearing currentUser
    env.setUser(null);

    // toggleFavorite should complete without TypeError (.id on null)
    await expect(togglePromise).resolves.toBeUndefined();
  });

  test('removing and adding the same player yields consistent final state', async () => {
    const env = makeEnv();
    env.setUser({ id: 'u1', email: 'a@b.com' });

    await env.toggleFavorite('p1');  // add
    expect(env.getFavorites().has('p1')).toBe(true);

    await env.toggleFavorite('p1');  // remove
    expect(env.getFavorites().has('p1')).toBe(false);

    await env.toggleFavorite('p1');  // add again
    expect(env.getFavorites().has('p1')).toBe(true);
  });
});
