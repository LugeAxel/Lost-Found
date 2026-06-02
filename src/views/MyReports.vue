<script setup lang="ts">
import { ref, computed, onMounted } from 'vue';
import { useHead } from '@unhead/vue';
import axios from 'axios';
import { useRouter } from 'vue-router';
import QrcodeVue from 'qrcode.vue';
import SideNav from '../components/SideNav.vue';
import TopNav from '../components/TopNav.vue';
import ItemSkeleton from '../components/ItemSkeleton.vue';
import { API_URL } from '@/config/api';
import { useToast } from '../composables/useToast';
import { getAuthHeaders } from '../composables/useAuth';
import { supabase } from '../lib/supabase';
import { useI18n } from '../i18n';
import { optimizeImageUrl } from '../utils/cloudinary';
import { useRealtimeItems } from '../composables/useRealtimeItems';

const router = useRouter();
const toast = useToast();
const { t } = useI18n();
useHead({ title: () => `${t('myreports.title')} — QReturn` });
const items = ref<any[]>([]);
const isLoading = ref(true);
const itemMatches = ref<Record<string, any[]>>({});
const filterAreaCategory = ref('all');

const showEditModal = ref(false);
const isSavingEdit = ref(false);
const editingItem = ref<any>(null);
const editForm = ref({
  name: '',
  location: '',
  category: '',
  area_category: '',
  type: '',
  description: '',
});
const editImagePreview = ref<string | null>(null);
const editImageFile = ref<File | null>(null);
const editFileInput = ref<HTMLInputElement | null>(null);
const removeImage = ref(false);

const { subscribe: subscribeItems } = useRealtimeItems(() => {
  fetchMyReports();
});

const areaCategories = [
  'Ruang Teori', 'Laboratorium', 'Masjid', 'Kantin', 'Koperasi',
  'Bima', 'Yudhistira', 'Arjuna', 'Lapangan', 'Kantor',
  'Parkir', 'Bengkel', 'Bangunan Kimia', 'Bangunan GP'
];

const filteredItems = computed(() => {
  if (filterAreaCategory.value === 'all') return items.value;
  return items.value.filter(i => i.area_category === filterAreaCategory.value);
});

const fetchMyReports = async () => {
  try {
    // Uses the new /api/items/mine endpoint — backend scopes to authenticated user
    const res = await axios.get(`${API_URL}/api/items/mine`, { headers: await getAuthHeaders() });
    items.value = res.data;
    await fetchSuggestions();
  } catch (error: any) {
    if (error.response?.status === 401) {
      await supabase.auth.signOut();
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      router.push('/');
    }
    console.error('Error fetching my reports:', error);
  } finally {
    isLoading.value = false;
  }
};

const fetchSuggestions = async () => {
  try {
    const lostItems = items.value.filter((i: any) => i.type === 'lost' && i.status !== 'Returned');
    if (lostItems.length === 0) {
      itemMatches.value = {};
      return;
    }
    const headers = await getAuthHeaders();
    const results = await Promise.allSettled(
      lostItems.map((item: any) =>
        axios.get(`${API_URL}/api/items/matches/${item.id}`, { headers })
          .then(res => ({ itemId: item.id, matches: res.data }))
      )
    );
    const map: Record<string, any[]> = {};
    for (const result of results) {
      if (result.status === 'fulfilled' && result.value.matches?.length > 0) {
        map[result.value.itemId] = result.value.matches;
      }
    }
    itemMatches.value = map;
  } catch (error) {
    console.error('Error fetching suggestions:', error);
  }
};

onMounted(async () => {
  await fetchMyReports();
  const { data: { session } } = await supabase.auth.getSession();
  if (session?.user?.id) {
    subscribeItems(session.user.id);
  }
});

const getClaimUrl = (code: string) => {
  return `${window.location.origin}/claim/${code}`;
};

const formatDate = (date: string) => {
  return new Date(date).toLocaleDateString('id-ID', { day: 'numeric', month: 'long', year: 'numeric' });
};

const copyText = async (text: string) => {
  try {
    await navigator.clipboard.writeText(text);
    toast.show(t('myreports.copied'), 'success');
  } catch {
    toast.show('Failed to copy', 'error');
  }
};

const openEdit = (item: any) => {
  editingItem.value = item;
  editForm.value = {
    name: item.name || '',
    location: item.location || '',
    category: item.category || '',
    area_category: item.area_category || '',
    type: item.type || '',
    description: item.description || '',
  };
  editImagePreview.value = item.image_url || null;
  editImageFile.value = null;
  removeImage.value = false;
  showEditModal.value = true;
};

const closeEdit = () => {
  showEditModal.value = false;
  editingItem.value = null;
  editImagePreview.value = null;
  editImageFile.value = null;
  removeImage.value = false;
};

const handleEditImage = (event: Event) => {
  const target = event.target as HTMLInputElement;
  if (target.files && target.files[0]) {
    editImageFile.value = target.files[0];
    removeImage.value = false;
    const reader = new FileReader();
    reader.onload = (e) => {
      editImagePreview.value = e.target?.result as string;
    };
    reader.readAsDataURL(target.files[0]);
  }
};

const handleRemoveImage = () => {
  editImagePreview.value = null;
  editImageFile.value = null;
  removeImage.value = true;
};

const removeClaimer = async () => {
  if (!confirm('Remove the current claimer? The item status will be set back to Available.')) return;
  try {
    await axios.put(`${API_URL}/api/items/${editingItem.value.id}`,
      { remove_claimer: true },
      { headers: await getAuthHeaders() }
    );
    toast.show('Claimer removed successfully', 'success');
    await fetchMyReports();
    closeEdit();
  } catch (error: any) {
    toast.show(error.response?.data?.message || 'Failed to remove claimer', 'error');
  }
};

const saveEdit = async () => {
  if (!editForm.value.name.trim()) {
    toast.show('Item name is required', 'error');
    return;
  }
  if (!editForm.value.location.trim()) {
    toast.show('Location is required', 'error');
    return;
  }

  isSavingEdit.value = true;
  try {
    const hasNewImage = editImageFile.value != null;
    const wantsRemove = removeImage.value && !hasNewImage;
    let payload;

    if (hasNewImage) {
      const fd = new FormData();
      fd.append('name', editForm.value.name);
      fd.append('location', editForm.value.location);
      fd.append('category', editForm.value.category);
      fd.append('area_category', editForm.value.area_category || '');
      fd.append('type', editForm.value.type);
      fd.append('description', editForm.value.description);
      fd.append('image', editImageFile.value!);
      if (wantsRemove) fd.append('remove_image', 'true');
      payload = fd;
    } else {
      payload = { ...editForm.value, ...(wantsRemove ? { remove_image: true } : {}) };
    }

    const res = await axios.put(`${API_URL}/api/items/${editingItem.value.id}`,
      payload,
      { headers: await getAuthHeaders() }
    );

    if (res.data?.item) {
      const updatedItem = res.data.item;
      if (hasNewImage && updatedItem.image_url === editingItem.value?.image_url) {
        toast.show('Item saved but image may not have changed', 'info');
      } else {
        toast.show('Item updated successfully', 'success');
      }
    } else {
      toast.show('Item updated successfully', 'success');
    }
    await fetchMyReports();
    closeEdit();
  } catch (error: any) {
    const data = error.response?.data;
    const msg = data?.message || 'Failed to update item';
    const code = data?.code || '';
    const details = data?.details || '';
    const fullMsg = code ? `${msg} (${code})` : msg;
    console.error('Edit error:', { message: data?.message || error.message, code, details });
    toast.show(details ? `${msg}: ${details}` : fullMsg, 'error');
  } finally {
    isSavingEdit.value = false;
  }
};
</script>

<template>
  <div class="min-h-screen bg-[#f8faf7] dark:bg-[#121212] flex font-sans">
    <SideNav />
    <TopNav />

    <main class="md:ml-64 pt-24 px-4 sm:px-6 md:px-8 pb-24 md:pb-12 flex-1 min-w-0">
      <div class="flex justify-between items-center mb-10">
        <div>
          <h2 class="text-3xl font-bold text-[#1c1b1b] dark:text-[#f3f4f6] tracking-tight">{{ t('myreports.title') }}</h2>
          <p class="text-[#40493d] dark:text-[#9ca3af] text-sm">{{ t('myreports.subtitle') }}</p>
        </div>
        <RouterLink to="/report" class="px-6 py-3 bg-[#387b41] text-white rounded-xl font-bold flex items-center gap-2 shadow-md hover:bg-[#2d6334] transition-all active:scale-95 text-sm">
          <span class="material-symbols-outlined">add</span> {{ t('myreports.new_report') }}
        </RouterLink>
      </div>

      <div class="relative sm:w-fit min-w-[220px] w-fit mb-10">
          <span class="material-symbols-outlined absolute left-5 top-1/2 -translate-y-1/2 text-slate-700 dark:text-[#9ca3af] text-lg pointer-events-none">business</span>
          <select v-model="filterAreaCategory"
            class="w-full bg-[#f3f5f2] dark:bg-[#2a2a2a] dark:text-white border-2 border-transparent rounded-xl px-5 py-3 pl-13 focus:border-[#387b41] focus:bg-white dark:focus:bg-[#1e1e1e] outline-none transition-all text-xs font-medium appearance-none cursor-pointer">
            <option value="all">{{ t('myreports.all_areas') }}</option>
            <option v-for="area in areaCategories" :key="area" :value="area">{{ t('area.' + area) }}</option>
          </select>
          <span class="material-symbols-outlined absolute right-4 top-1/2 -translate-y-1/2 pointer-events-none text-[#40493d] dark:text-[#9ca3af]">expand_more</span>
        </div>

      <div v-if="isLoading" class="grid grid-cols-1 gap-6">
        <ItemSkeleton v-for="i in 4" :key="i" />
      </div>

      <div v-else-if="filteredItems.length === 0" class="text-center py-32 bg-white dark:bg-[#1e1e1e] rounded-[2.5rem] border border-dashed border-[#e0e4df] dark:border-[#374151]">
        <span class="material-symbols-outlined text-7xl text-[#40493d] dark:text-[#9ca3af]/10 mb-6">inventory_2</span>
          <h3 class="text-xl font-bold text-[#1c1b1b] dark:text-[#f3f4f6] mb-2">{{ t('myreports.no_reports') }}</h3>
        <p class="text-[#40493d] dark:text-[#9ca3af] max-w-xs mx-auto">{{ t('myreports.no_reports_desc') }}</p>
      </div>

      <div v-else class="grid grid-cols-1 gap-4">
        <div v-for="item in filteredItems" :key="item.id" @click="router.push(`/item/${item.id}`)" class="bg-white dark:bg-[#1e1e1e] rounded-xl p-4 md:p-5 border border-[#e0e4df] dark:border-[#374151] shadow-sm hover:shadow-md transition-all relative overflow-hidden cursor-pointer">
          <div v-if="item.status === 'Returned'" class="absolute top-0 right-0 z-10 overflow-hidden w-24 h-24">
            <span class="absolute top-3 right-[-30px] w-28 py-1 bg-[#387b41] text-white text-[10px] font-bold uppercase tracking-widest text-center rotate-45 shadow-sm">{{ t('card.returned') }}</span>
          </div>
          
          <div class="flex flex-col md:flex-row gap-4 md:gap-5">
            <div class="w-full md:w-36 h-36 rounded-xl overflow-hidden bg-[#f3f5f2] dark:bg-[#2a2a2a] flex-shrink-0 relative group">
              <img v-if="item.image_url" :src="optimizeImageUrl(item.image_url)" class="w-full h-full object-cover" />
              <div v-else class="absolute inset-0 flex items-center justify-center">
                <span class="material-symbols-outlined text-4xl text-[#40493d] dark:text-[#9ca3af]/40">{{ item.type === 'lost' ? 'search' : 'inventory_2' }}</span>
              </div>
            </div>

            <div class="flex-1 flex flex-col justify-between py-1">
              <div>
                <div class="flex items-center gap-2 mb-2.5">
                  <span :class="['text-[10px] px-3 py-1 rounded-full font-bold tracking-wider', item.type === 'lost' ? 'bg-[#fef2f2] text-[#ba1a1a]' : 'bg-[#f0fdf4] text-[#387b41]']">
                    {{ item.type === 'lost' ? t('card.lost') : t('card.found') }}
                  </span>
                  <span :class="['text-[10px] px-3 py-1 rounded-full font-bold', item.status === 'Available' ? 'bg-[#abf4ac] text-[#07521d]' : item.status === 'On Progress' ? 'bg-[#ffecb3] text-[#f57f17]' : 'bg-[#dee5d6] text-[#42493e]']">
                    {{ t('card.status.' + item.status) }}
                  </span>
                  <button v-if="item.status !== 'Returned'" @click.stop="openEdit(item)"
                    class="ml-auto w-7 h-7 rounded-full bg-[#f3f5f2] dark:bg-[#2a2a2a] flex items-center justify-center hover:bg-[#e0e4df] dark:hover:bg-[#374151] hover:text-[#387b41] transition-all text-[#40493d] dark:text-[#9ca3af]"
                    title="Edit item">
                    <span class="material-symbols-outlined text-sm">edit</span>
                  </button>
                </div>
                <h4 class="text-xl font-bold text-[#1c1b1b] dark:text-[#f3f4f6] mb-1.5">{{ item.name }}</h4>
                <div class="flex flex-wrap gap-x-4 gap-y-1.5">
                  <p class="text-sm text-[#40493d] dark:text-[#9ca3af] flex items-center gap-1"><span class="material-symbols-outlined text-lg">location_on</span> {{ item.type === 'lost' ? t('detail.last_seen') + ' ' + item.location : t('detail.found_at') + ' ' + item.location }}</p>
                  <p class="text-sm text-[#40493d] dark:text-[#9ca3af] flex items-center gap-1"><span class="material-symbols-outlined text-lg">calendar_today</span> {{ formatDate(item.reported_at) }}</p>
                  <p class="text-sm text-[#40493d] dark:text-[#9ca3af] flex items-center gap-1"><span class="material-symbols-outlined text-lg">category</span> {{ t('card.category.' + item.category) }}</p>
                </div>
                <div v-if="item.coordinates_lat != null && item.coordinates_lng != null" class="mt-1.5 flex items-center gap-2">
                  <span class="text-[10px] text-[#387b41] font-bold flex items-center gap-1">
                    <span class="material-symbols-outlined text-xs">gps_fixed</span>
                    {{ item.coordinates_lat.toFixed(4) }}, {{ item.coordinates_lng.toFixed(4) }}
                  </span>
                  <button @click="copyText(`${item.coordinates_lat},${item.coordinates_lng}`)"
                    class="text-[10px] text-[#387b41] hover:text-[#2d6334] font-bold flex items-center gap-0.5 transition-colors"
                    title="Copy GPS coordinates">
                    <span class="material-symbols-outlined text-xs">content_copy</span>
                  </button>
                </div>
                <p class="mt-3 text-sm text-[#40493d] dark:text-[#9ca3af] italic leading-relaxed" v-if="item.description">"{{ item.description }}"</p>
              </div>

              <!-- Proof of Return -->
              <div v-if="item.status === 'Returned' && item.claim_photo" class="mt-4 pt-4 border-t border-[#e0e4df] dark:border-[#374151]">
                <p class="text-[10px] font-bold text-[#40493d] dark:text-[#9ca3af] mb-3 uppercase tracking-wider">{{ t('myreports.proof_of_return') }}</p>
                <div class="flex gap-4 items-center">
                  <img :src="item.claim_photo" class="w-16 h-16 rounded-lg object-cover border border-[#e0e4df] dark:border-[#374151]" loading="lazy" />
                  <div>
                    <p class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6]">{{ t('myreports.verified_return') }}</p>
                    <p class="text-[10px] text-[#40493d] dark:text-[#9ca3af]">{{ item.claim_notes || t('myreports.handed_over') }}</p>
                  </div>
                </div>
              </div>
            </div>

            <div v-if="item.status !== 'Returned'" class="flex flex-col items-center justify-center p-3 md:p-4 bg-[#f3f5f2] dark:bg-[#2a2a2a] rounded-xl border border-[#e0e4df] dark:border-[#374151] min-w-[160px]">
              <p class="text-[10px] font-bold text-[#40493d] dark:text-[#9ca3af] mb-2 uppercase tracking-[0.2em]">{{ t('myreports.claim_qr_code') }}</p>
              <div class="p-2 bg-white rounded-xl shadow-inner mb-3">
                <qrcode-vue :value="getClaimUrl(item.claim_code)" :size="80" level="H" foreground="#1c1b1b" class=""/>
              </div>
              <div class="mb-2 flex items-center gap-1 bg-[#e0e4df] dark:bg-[#374151] px-3 py-1.5 rounded-lg">
                <span class="text-[9px] font-mono text-[#40493d] dark:text-[#9ca3af] truncate max-w-[120px]">{{ item.claim_code }}</span>
                <button @click="copyText(item.claim_code)"
                  class="text-[#387b41] hover:text-[#2d6334] transition-colors shrink-0"
                  title="Copy item ID">
                  <span class="material-symbols-outlined text-sm">content_copy</span>
                </button>
              </div>
              <p v-if="item.status === 'On Progress'" class="text-[9px] text-[#f57f17] text-center font-bold max-w-[140px] mb-2">{{ t('myreports.claim_in_progress') }}</p>
              <p class="text-[9px] text-[#40493d] dark:text-[#9ca3af] text-center font-medium max-w-[140px]">{{ t('myreports.claim_qr_desc') }}</p>
            </div>
            <div v-else class="flex flex-col items-center justify-center p-3 md:p-4 bg-[#f0fdf4] rounded-xl border border-[#abf4ac] min-w-[160px]">
              <span class="material-symbols-outlined text-4xl text-[#387b41] mb-2">check_circle</span>
              <p class="text-xs font-bold text-[#387b41]">{{ t('myreports.returned_successfully') }}</p>
            </div>
          </div>

          <!-- This Might Be Your Item (per lost item matches) -->
          <div v-if="item.type === 'lost' && itemMatches[item.id]?.length" class="mt-4 pt-4 border-t border-[#e0e4df] dark:border-[#374151]">
            <div class="flex items-center gap-2 mb-4">
              <span class="material-symbols-outlined text-[#f57f17] text-base">lightbulb</span>
              <h4 class="text-sm font-bold text-[#1c1b1b] dark:text-[#f3f4f6]">{{ t('myreports.suggestions_title') }}</h4>
            </div>
            <div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-[repeat(auto-fill,minmax(240px,1fr))] gap-3">
              <div v-for="match in itemMatches[item.id]" :key="match.id" @click="router.push(`/item/${match.id}`)"
                class="bg-[#f8faf7] dark:bg-[#121212] rounded-xl border border-[#e0e4df] dark:border-[#374151] overflow-hidden hover:shadow-md hover:border-[#387b41]/30 transition-all cursor-pointer group flex gap-4 p-3">
                <div class="w-20 h-20 rounded-lg bg-[#f3f5f2] dark:bg-[#2a2a2a] overflow-hidden flex-shrink-0">
                  <img v-if="match.image_url" :src="optimizeImageUrl(match.image_url)" :class="['w-full h-full object-cover', match.status === 'Returned' && 'grayscale opacity-70']" />
                  <div v-else class="w-full h-full flex items-center justify-center">
                    <span class="material-symbols-outlined text-xl text-[#40493d] dark:text-[#9ca3af]/20">inventory_2</span>
                  </div>
                </div>
                <div class="flex-1 min-w-0 flex flex-col justify-center">
                  <div class="flex items-center gap-1.5 mb-1 flex-wrap">
                    <span class="text-sm font-bold text-[#1c1b1b] dark:text-[#f3f4f6] truncate">{{ match.name }}</span>
                    <span class="px-2 py-0.5 bg-[#387b41]/10 text-[#387b41] text-[10px] font-bold rounded-full uppercase shrink-0">{{ t('myreports.suggestions_match') }}</span>
                    <span v-if="match.status === 'Available'" class="px-2 py-0.5 bg-[#abf4ac] text-[#07521d] text-[10px] ml-1 font-bold rounded-full uppercase shrink-0">{{ t('card.status.Available') }}</span>
                    <span v-else-if="match.status === 'On Progress'" class="px-2 py-0.5 bg-[#ffecb3] text-[#f57f17] text-[10px] ml-1 font-bold rounded-full uppercase shrink-0">{{ t('card.status.On Progress') }}</span>
                    <span v-else-if="match.status === 'Returned'" class="px-2 py-0.5 bg-[#dee5d6] text-[#42493e] text-[10px] ml-1 font-bold rounded-full uppercase shrink-0">{{ t('card.status.Returned') }}</span>
                  </div>
                  <p class="text-xs text-[#40493d] dark:text-[#9ca3af] truncate flex items-center gap-1">
                    <span class="material-symbols-outlined text-[10px]">location_on</span>
                    {{ match.location }}
                  </p>
                  <p v-if="match.distance_meters != null" class="text-xs text-[#387b41] font-bold mt-0.5 flex items-center gap-1">
                    <span class="material-symbols-outlined text-[10px]">near_me</span>
                    {{ match.distance_meters }}m {{ t('myreports.suggestions_distance') }}
                  </p>
                  <p v-else class="text-xs text-[#ba1a1a] font-bold mt-0.5 flex items-center gap-1">
                    <span class="material-symbols-outlined text-[10px]">gps_off</span>
                    {{ t('card.no_gps') }}
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </main>

    <!-- Edit Item Modal -->
    <Teleport to="body">
      <div v-if="showEditModal" class="fixed inset-0 z-50 flex items-center justify-center p-4" @click.self="closeEdit">
        <div class="absolute inset-0 bg-black/50 backdrop-blur-sm"></div>
        <div class="relative bg-white dark:bg-[#1e1e1e] rounded-[2rem] w-full max-w-lg max-h-[90vh] overflow-y-auto border border-[#e0e4df] dark:border-[#374151] shadow-2xl animate-fade-in">
          <div class="sticky top-0 bg-white dark:bg-[#1e1e1e] p-6 pb-4 border-b border-[#e0e4df] dark:border-[#374151] flex items-center justify-between rounded-t-[2rem] z-10">
            <h3 class="text-lg font-bold text-[#1c1b1b] dark:text-[#f3f4f6]">{{ t('myreports.edit_item') }}</h3>
            <button @click="closeEdit" class="w-8 h-8 rounded-full bg-[#f3f5f2] dark:bg-[#2a2a2a] flex items-center justify-center hover:bg-[#e0e4df] dark:hover:bg-[#374151] transition-colors">
              <span class="material-symbols-outlined text-sm">close</span>
            </button>
          </div>

          <div class="p-6 space-y-5">
            <div class="space-y-1.5">
              <label class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6] uppercase tracking-wider">{{ t('myreports.name') }}</label>
              <input v-model="editForm.name" type="text" class="w-full bg-[#f3f5f2] dark:bg-[#2a2a2a] dark:text-white border-2 border-transparent rounded-xl px-4 py-3 focus:border-[#387b41] focus:bg-white dark:focus:bg-[#1e1e1e] outline-none transition-all text-sm font-medium" />
            </div>

            <div class="space-y-1.5">
              <label class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6] uppercase tracking-wider">{{ t('myreports.location') }}</label>
              <input v-model="editForm.location" type="text" class="w-full bg-[#f3f5f2] dark:bg-[#2a2a2a] dark:text-white border-2 border-transparent rounded-xl px-4 py-3 focus:border-[#387b41] focus:bg-white dark:focus:bg-[#1e1e1e] outline-none transition-all text-sm font-medium" />
            </div>

            <div class="space-y-1.5">
              <label class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6] uppercase tracking-wider">{{ t('myreports.type') }}</label>
              <div class="flex gap-2">
                <button @click="editForm.type = 'found'" :class="['flex-1 py-3 rounded-xl font-bold text-sm transition-all', editForm.type === 'found' ? 'bg-[#387b41] text-white shadow-sm' : 'bg-[#f3f5f2] dark:bg-[#2a2a2a] text-[#40493d] dark:text-[#9ca3af]']">{{ t('report.found') }}</button>
                <button @click="editForm.type = 'lost'" :class="['flex-1 py-3 rounded-xl font-bold text-sm transition-all', editForm.type === 'lost' ? 'bg-[#ba1a1a] text-white shadow-sm' : 'bg-[#f3f5f2] dark:bg-[#2a2a2a] text-[#40493d] dark:text-[#9ca3af]']">{{ t('report.lost') }}</button>
              </div>
            </div>

            <div class="space-y-1.5">
              <label class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6] uppercase tracking-wider">{{ t('myreports.category') }}</label>
              <div class="relative">
                <select v-model="editForm.category" class="w-full bg-[#f3f5f2] dark:bg-[#2a2a2a] dark:text-white border-2 border-transparent rounded-xl px-4 py-3 focus:border-[#387b41] focus:bg-white dark:focus:bg-[#1e1e1e] outline-none transition-all text-sm font-medium appearance-none">
                  <option v-for="cat in ['Electronics','Daily Use','Clothing','Books/Stationery','Others']" :key="cat" :value="cat">{{ t('card.category.' + cat) }}</option>
                </select>
                <span class="material-symbols-outlined absolute right-4 top-1/2 -translate-y-1/2 pointer-events-none text-[#40493d] dark:text-[#9ca3af]">expand_more</span>
              </div>
            </div>

            <div class="space-y-1.5">
              <label class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6] uppercase tracking-wider">{{ t('myreports.area_category') }}</label>
              <div class="relative">
                <select v-model="editForm.area_category" class="w-full bg-[#f3f5f2] dark:bg-[#2a2a2a] dark:text-white border-2 border-transparent rounded-xl px-4 py-3 focus:border-[#387b41] focus:bg-white dark:focus:bg-[#1e1e1e] outline-none transition-all text-sm font-medium appearance-none">
                  <option value="">{{ t('myreports.select_area') }}</option>
                  <option v-for="area in areaCategories" :key="area" :value="area">{{ t('area.' + area) }}</option>
                </select>
                <span class="material-symbols-outlined absolute right-4 top-1/2 -translate-y-1/2 pointer-events-none text-[#40493d] dark:text-[#9ca3af]">expand_more</span>
              </div>
            </div>

            <div class="space-y-1.5">
              <label class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6] uppercase tracking-wider">{{ t('myreports.description') }}</label>
              <textarea v-model="editForm.description" rows="3" class="w-full bg-[#f3f5f2] dark:bg-[#2a2a2a] dark:text-white dark:placeholder-gray-500 border-2 border-transparent rounded-xl px-4 py-3 focus:border-[#387b41] focus:bg-white dark:focus:bg-[#1e1e1e] outline-none transition-all text-sm font-medium resize-none"></textarea>
            </div>

            <div class="space-y-1.5">
              <label class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6] uppercase tracking-wider">{{ t('myreports.image') }}</label>
              <div class="flex items-center gap-4">
                <div v-if="editImagePreview" class="w-20 h-20 rounded-xl overflow-hidden bg-[#f3f5f2] dark:bg-[#2a2a2a] border border-[#e0e4df] dark:border-[#374151] flex-shrink-0">
                  <img :src="editImagePreview" class="w-full h-full object-cover" loading="lazy" />
                </div>
                <div class="flex flex-wrap gap-2">
                  <button @click="editFileInput?.click()" type="button" class="px-4 py-2 bg-[#f3f5f2] dark:bg-[#2a2a2a] rounded-xl text-xs font-bold text-[#40493d] dark:text-[#9ca3af] hover:bg-[#e0e4df] dark:hover:bg-[#374151] transition-all">
                    {{ editImagePreview ? t('myreports.change_photo') : t('myreports.upload_photo') }}
                  </button>
                  <button v-if="editImagePreview" @click="handleRemoveImage" type="button" class="px-4 py-2 bg-red-50 dark:bg-red-950/20 rounded-xl text-xs font-bold text-[#ba1a1a] hover:bg-red-100 dark:hover:bg-red-950/40 transition-all">
                    {{ t('myreports.remove_photo') }}
                  </button>
                </div>
                <input ref="editFileInput" type="file" accept="image/*" class="hidden" @change="handleEditImage" />
              </div>
            </div>

            <div v-if="editingItem?.status === 'On Progress' && editingItem?.claimer" class="space-y-1.5 pt-4 border-t border-[#e0e4df] dark:border-[#374151]">
              <label class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6] uppercase tracking-wider">{{ t('myreports.claimer') }}</label>
              <div class="flex items-center justify-between p-3 bg-red-50 dark:bg-red-950/20 rounded-xl border border-red-100 dark:border-red-900/30">
                <div class="flex items-center gap-2">
                  <span class="material-symbols-outlined text-[#ba1a1a] text-sm">person</span>
                  <span class="text-xs font-bold text-[#1c1b1b] dark:text-[#f3f4f6]">{{ editingItem?.claimer?.nama || t('myreports.claimer') }}</span>
                </div>
                <button @click="removeClaimer" class="px-4 py-2 bg-[#ba1a1a] text-white rounded-lg text-xs font-bold hover:bg-[#991515] transition-all">{{ t('myreports.remove_claimer') }}</button>
              </div>
            </div>
          </div>

          <div class="sticky bottom-0 bg-white dark:bg-[#1e1e1e] p-6 pt-4 border-t border-[#e0e4df] dark:border-[#374151] flex gap-3">
            <button @click="closeEdit" class="flex-1 py-3 bg-[#f3f5f2] dark:bg-[#2a2a2a] text-[#40493d] dark:text-[#9ca3af] rounded-xl font-bold text-sm hover:bg-[#e0e4df] dark:hover:bg-[#374151] transition-all">{{ t('myreports.cancel') }}</button>
            <button @click="saveEdit" :disabled="isSavingEdit" class="flex-1 py-3 bg-[#387b41] text-white rounded-xl font-bold text-sm hover:bg-[#2d6334] transition-all disabled:opacity-50 disabled:pointer-events-none">
              {{ isSavingEdit ? t('myreports.saving') : t('myreports.save_changes') }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<style scoped>
@keyframes fade-in {
  from { opacity: 0; transform: translateY(8px) scale(0.97); }
  to { opacity: 1; transform: translateY(0) scale(1); }
}
.animate-fade-in {
  animation: fade-in 0.2s ease-out;
}
</style>
