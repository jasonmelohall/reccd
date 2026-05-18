import React, { useState } from 'react';
import {
  ActivityIndicator,
  KeyboardAvoidingView,
  Platform,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from 'react-native';
import Constants from 'expo-constants';

const API_BASE_URL =
  Constants?.expoConfig?.extra?.apiBaseUrl ??
  Constants?.manifest?.extra?.apiBaseUrl ??
  'https://reccd-web-service.onrender.com';

const DEFAULT_USER_ID = 1;

const SearchScreen = ({ navigation }) => {
  const [searchMode, setSearchMode] = useState('genai'); // 'regular' | 'genai'
  const [searchTerm, setSearchTerm] = useState('');
  const [userInput, setUserInput] = useState('');
  const [numTerms, setNumTerms] = useState(3);
  const [statusMessage, setStatusMessage] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSearch = async () => {
    setError('');
    setStatusMessage('');
    setLoading(true);

    try {
      if (searchMode === 'genai') {
        const raw = (userInput || '').trim();
        if (!raw) {
          setError('Describe what you\'re looking for to continue.');
          setLoading(false);
          return;
        }
        const n = Math.max(1, Math.min(10, numTerms));
        const response = await fetch(`${API_BASE_URL}/api/search`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            genai: true,
            user_input: raw,
            num_terms: n,
            user_id: DEFAULT_USER_ID,
          }),
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data?.detail || 'Request failed');
        setStatusMessage(data?.message || 'Analyzing products...');
        navigation.navigate('Results', {
          searchTerm: data?.search_terms?.[0] ?? raw,
          searchTerms: data?.search_terms ?? [raw],
          userId: DEFAULT_USER_ID,
          status: data?.status,
          itemsFound: data?.items_found,
        });
      } else {
        const trimmed = searchTerm.trim();
        if (!trimmed) {
          setError('Enter a search term to continue.');
          setLoading(false);
          return;
        }
        const response = await fetch(`${API_BASE_URL}/api/search`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            search_term: trimmed,
            user_id: DEFAULT_USER_ID,
          }),
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data?.detail || 'Request failed');
        setStatusMessage(data?.message || 'Analyzing products...');
        navigation.navigate('Results', {
          searchTerm: trimmed,
          userId: DEFAULT_USER_ID,
          status: data?.status,
          itemsFound: data?.items_found,
        });
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <KeyboardAvoidingView
      style={styles.container}
      behavior={Platform.OS === 'ios' ? 'padding' : undefined}
    >
      <View style={styles.card}>
        <Text style={styles.title}>Find Amazon best-sellers</Text>
        <Text style={styles.subtitle}>
          Reccd runs the full ML pipeline (Rainforest + Keepa + regression) every time you search.
        </Text>

        <View style={styles.toggleRow}>
          <TouchableOpacity
            style={[styles.toggleBtn, searchMode === 'regular' && styles.toggleBtnActive]}
            onPress={() => setSearchMode('regular')}
          >
            <Text style={[styles.toggleText, searchMode === 'regular' && styles.toggleTextActive]}>
              Regular Search
            </Text>
          </TouchableOpacity>
          <TouchableOpacity
            style={[styles.toggleBtn, searchMode === 'genai' && styles.toggleBtnActive]}
            onPress={() => setSearchMode('genai')}
          >
            <Text style={[styles.toggleText, searchMode === 'genai' && styles.toggleTextActive]}>
              GenAI Search
            </Text>
          </TouchableOpacity>
        </View>

        {searchMode === 'regular' ? (
          <TextInput
            style={styles.input}
            placeholder="e.g. champagne gold bathroom trash can"
            value={searchTerm}
            onChangeText={setSearchTerm}
            autoCorrect={false}
            autoCapitalize="none"
            returnKeyType="search"
            onSubmitEditing={handleSearch}
          />
        ) : (
          <>
            <TextInput
              style={[styles.input, styles.textArea]}
              placeholder="Describe what you need in a few lines... e.g. Small bathroom trash can, champagne gold, with lid, modern look"
              value={userInput}
              onChangeText={setUserInput}
              multiline
              numberOfLines={4}
              autoCorrect={false}
            />
            <View style={styles.numTermsRow}>
              <Text style={styles.numTermsLabel}>Number of search terms:</Text>
              <TextInput
                style={styles.numTermsInput}
                value={String(numTerms)}
                onChangeText={(t) => {
                  const n = parseInt(t, 10);
                  if (!isNaN(n)) setNumTerms(Math.max(1, Math.min(10, n)));
                }}
                keyboardType="number-pad"
              />
              <Text style={styles.numTermsHint}>1–10</Text>
            </View>
          </>
        )}

        <TouchableOpacity style={styles.button} onPress={handleSearch} disabled={loading}>
          {loading ? (
            <ActivityIndicator color="#fff" />
          ) : (
            <Text style={styles.buttonText}>Run Search</Text>
          )}
        </TouchableOpacity>

        {!!statusMessage && <Text style={styles.status}>{statusMessage}</Text>}
        {!!error && <Text style={styles.error}>{error}</Text>}
      </View>
    </KeyboardAvoidingView>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#232F3E',
    justifyContent: 'center',
    paddingHorizontal: 20,
  },
  card: {
    backgroundColor: '#fff',
    borderRadius: 16,
    padding: 24,
    shadowColor: '#000',
    shadowOpacity: 0.1,
    shadowRadius: 20,
    shadowOffset: { width: 0, height: 10 },
    elevation: 5,
  },
  title: {
    fontSize: 24,
    fontWeight: 'bold',
    marginBottom: 8,
    color: '#111',
  },
  subtitle: {
    fontSize: 15,
    color: '#4A5568',
    marginBottom: 20,
  },
  toggleRow: {
    flexDirection: 'row',
    marginBottom: 16,
    gap: 8,
  },
  toggleBtn: {
    flex: 1,
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 10,
    backgroundColor: '#E2E8F0',
    alignItems: 'center',
  },
  toggleBtnActive: {
    backgroundColor: '#FF9900',
  },
  toggleText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#4A5568',
  },
  toggleTextActive: {
    color: '#111',
  },
  input: {
    borderWidth: 1,
    borderColor: '#E2E8F0',
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 12,
    fontSize: 16,
    marginBottom: 16,
  },
  textArea: {
    minHeight: 88,
    textAlignVertical: 'top',
  },
  numTermsRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 16,
    gap: 8,
  },
  numTermsLabel: {
    fontSize: 14,
    color: '#4A5568',
  },
  numTermsInput: {
    borderWidth: 1,
    borderColor: '#E2E8F0',
    borderRadius: 8,
    paddingHorizontal: 12,
    paddingVertical: 8,
    fontSize: 16,
    width: 48,
    textAlign: 'center',
  },
  numTermsHint: {
    fontSize: 12,
    color: '#718096',
  },
  button: {
    backgroundColor: '#FF9900',
    borderRadius: 12,
    paddingVertical: 14,
    alignItems: 'center',
  },
  buttonText: {
    color: '#111',
    fontWeight: '600',
    fontSize: 16,
  },
  status: {
    marginTop: 16,
    color: '#2F855A',
    fontWeight: '500',
  },
  error: {
    marginTop: 16,
    color: '#E53E3E',
    fontWeight: '500',
  },
});

export default SearchScreen;
