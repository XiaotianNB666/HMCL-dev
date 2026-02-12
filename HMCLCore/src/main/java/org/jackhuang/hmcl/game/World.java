/*
 * Hello Minecraft! Launcher
 * Copyright (C) 2020  huangyuhui <huanghongxun2008@126.com> and contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.jackhuang.hmcl.game;

import com.github.steveice10.opennbt.NBTIO;
import com.github.steveice10.opennbt.tag.builtin.*;
import javafx.scene.image.Image;
import org.jackhuang.hmcl.util.io.*;
import org.jackhuang.hmcl.util.versioning.GameVersionNumber;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;

import static org.jackhuang.hmcl.util.logging.Logger.LOG;

public final class World {

    private final Path file;
    private String fileName;
    private CompoundTag levelData;
    private Image icon;
    private Path levelDataPath;

    public World(Path file) throws IOException {
        this.file = file;

        if (Files.isDirectory(file)) {
            loadFromDirectory();
        } else if (Files.isRegularFile(file)) {
            loadFromZip();
        } else {
            throw new IOException("Path " + file + " cannot be recognized as a Minecraft world");
        }
    }

    public Path getFile() {
        return file;
    }

    public String getFileName() {
        return fileName;
    }

    public String getWorldName() {
        CompoundTag data = levelData.get("Data");
        StringTag levelNameTag = data.get("LevelName");
        return levelNameTag.getValue();
    }

    public void setWorldName(String worldName) throws IOException {
        if (levelData.get("Data") instanceof CompoundTag data && data.get("LevelName") instanceof StringTag levelNameTag) {
            levelNameTag.setValue(worldName);
            writeLevelDat(levelData);
        }
    }

    public Path getLevelDatFile() {
        return file.resolve("level.dat");
    }

    public Path getSessionLockFile() {
        return file.resolve("session.lock");
    }

    public CompoundTag getLevelData() {
        return levelData;
    }

    public long getLastPlayed() {
        CompoundTag data = levelData.get("Data");
        LongTag lastPlayedTag = data.get("LastPlayed");
        return lastPlayedTag.getValue();
    }

    public @Nullable GameVersionNumber getGameVersion() {
        if (levelData.get("Data") instanceof CompoundTag data
                && data.get("Version") instanceof CompoundTag versionTag
                && versionTag.get("Name") instanceof StringTag nameTag) {
            return GameVersionNumber.asGameVersion(nameTag.getValue());
        }
        return null;
    }

    public @Nullable Long getSeed() {
        CompoundTag data = levelData.get("Data");
        if (data.get("WorldGenSettings") instanceof CompoundTag worldGenSettingsTag && worldGenSettingsTag.get("seed") instanceof LongTag seedTag) {
            return seedTag.getValue();
        } else if (data.get("RandomSeed") instanceof LongTag seedTag) {
            return seedTag.getValue();
        }
        return null;
    }

    public boolean isLargeBiomes() {
        CompoundTag data = levelData.get("Data");
        if (data.get("generatorName") instanceof StringTag generatorNameTag) {
            return "largeBiomes".equals(generatorNameTag.getValue());
        } else {
            if (data.get("WorldGenSettings") instanceof CompoundTag worldGenSettingsTag
                    && worldGenSettingsTag.get("dimensions") instanceof CompoundTag dimensionsTag
                    && dimensionsTag.get("minecraft:overworld") instanceof CompoundTag overworldTag
                    && overworldTag.get("generator") instanceof CompoundTag generatorTag) {
                if (generatorTag.get("biome_source") instanceof CompoundTag biomeSourceTag
                        && biomeSourceTag.get("large_biomes") instanceof ByteTag largeBiomesTag) {
                    return largeBiomesTag.getValue() == (byte) 1;
                } else if (generatorTag.get("settings") instanceof StringTag settingsTag) {
                    return "minecraft:large_biomes".equals(settingsTag.getValue());
                }
            }
            return false;
        }
    }

    public Image getIcon() {
        return icon;
    }

    public boolean isLocked() {
        return isLocked(getSessionLockFile());
    }

    public boolean supportDatapacks() {
        return getGameVersion() != null && getGameVersion().isAtLeast("1.13", "17w43a");
    }

    public boolean supportQuickPlay() {
        return getGameVersion() != null && getGameVersion().isAtLeast("1.20", "23w14a");
    }

    public static boolean supportQuickPlay(GameVersionNumber gameVersionNumber) {
        return gameVersionNumber != null && gameVersionNumber.isAtLeast("1.20", "23w14a");
    }

    private void loadFromDirectory() throws IOException {
        fileName = FileUtils.getName(file);
        Path levelDat = file.resolve("level.dat");
        if (!Files.exists(levelDat)) {
            levelDat = file.resolve("special_level.dat");
        }
        if (!Files.exists(levelDat)) {
            throw new IOException("Not a valid world directory since level.dat or special_level.dat cannot be found.");
        }
        loadAndCheckLevelDat(levelDat);
        this.levelDataPath = levelDat;

        Path iconFile = file.resolve("icon.png");
        if (Files.isRegularFile(iconFile)) {
            try (InputStream inputStream = Files.newInputStream(iconFile)) {
                icon = new Image(inputStream, 64, 64, true, false);
                if (icon.isError()) {
                    throw icon.getException();
                }
            } catch (Exception e) {
                LOG.warning("Failed to load world icon", e);
            }
        }
    }

    private void loadFromZipImpl(Path root) throws IOException {
        Path levelDat = root.resolve("level.dat");
        if (!Files.exists(levelDat)) {
            levelDat = root.resolve("special_level.dat");
        }
        if (!Files.exists(levelDat)) {
            throw new IOException("Not a valid world zip file since level.dat or special_level.dat cannot be found.");
        }
        loadAndCheckLevelDat(levelDat);

        Path iconFile = root.resolve("icon.png");
        if (Files.isRegularFile(iconFile)) {
            try (InputStream inputStream = Files.newInputStream(iconFile)) {
                icon = new Image(inputStream, 64, 64, true, false);
                if (icon.isError()) {
                    throw icon.getException();
                }
            } catch (Exception e) {
                LOG.warning("Failed to load world icon", e);
            }
        }
    }

    private void loadFromZip() throws IOException {
        try (FileSystem fs = CompressingUtils.readonly(file).setAutoDetectEncoding(true).build()) {
            Path levelDatPath = fs.getPath("/level.dat");
            if (Files.isRegularFile(levelDatPath)) {
                fileName = FileUtils.getName(file);
                loadFromZipImpl(fs.getPath("/"));
                return;
            }
            try (Stream<Path> stream = Files.list(fs.getPath("/"))) {
                Path root = stream.filter(Files::isDirectory).findAny()
                        .orElseThrow(() -> new IOException("Not a valid world zip file"));
                fileName = FileUtils.getName(root);
                loadFromZipImpl(root);
            }
        }
    }

    private void loadAndCheckLevelDat(Path levelDat) throws IOException {
        this.levelData = parseLevelDat(levelDat);
        CompoundTag data = levelData.get("Data");
        if (data == null) {
            throw new IOException("level.dat missing Data");
        }
        if (!(data.get("LevelName") instanceof StringTag)) {
            throw new IOException("level.dat missing LevelName");
        }
        if (!(data.get("LastPlayed") instanceof LongTag)) {
            throw new IOException("level.dat missing LastPlayed");
        }
    }

    public void reloadLevelDat() throws IOException {
        if (levelDataPath != null) {
            loadAndCheckLevelDat(this.levelDataPath);
        }
    }

    // The rename method is used to rename temporary world object during installation and copying,
    // so there is no need to modify the `file` field.
    public void rename(String newName) throws IOException {
        if (!Files.isDirectory(file)) {
            throw new IOException("Not a valid world directory");
        }

        // Change the name recorded in level.dat
        CompoundTag data = levelData.get("Data");
        data.put(new StringTag("LevelName", newName));
        writeLevelDat(levelData);

        // then change the folder's name
        Files.move(file, file.resolveSibling(newName));
    }

    public void install(Path savesDir, String name) throws IOException {
        Path worldDir;
        try {
            worldDir = savesDir.resolve(name);
        } catch (InvalidPathException e) {
            throw new IOException(e);
        }

        if (Files.isDirectory(worldDir)) {
            throw new FileAlreadyExistsException("World already exists");
        }

        if (Files.isRegularFile(file)) {
            try (FileSystem fs = CompressingUtils.readonly(file).setAutoDetectEncoding(true).build()) {
                Path levelDatPath = fs.getPath("/level.dat");
                if (Files.isRegularFile(levelDatPath)) {
                    fileName = FileUtils.getName(file);

                    new Unzipper(file, worldDir).unzip();
                } else {
                    try (Stream<Path> stream = Files.list(fs.getPath("/"))) {
                        List<Path> subDirs = stream.toList();
                        if (subDirs.size() != 1) {
                            throw new IOException("World zip malformed");
                        }
                        String subDirectoryName = FileUtils.getName(subDirs.get(0));
                        new Unzipper(file, worldDir)
                                .setSubDirectory("/" + subDirectoryName + "/")
                                .unzip();
                    }
                }

            }
            new World(worldDir).rename(name);
        } else if (Files.isDirectory(file)) {
            FileUtils.copyDirectory(file, worldDir);
        }
    }

    public void export(Path zip, String worldName) throws IOException {
        if (!Files.isDirectory(file)) {
            throw new IOException();
        }

        try (Zipper zipper = new Zipper(zip)) {
            zipper.putDirectory(file, worldName);
        }
    }

    public void delete() throws IOException {
        if (isLocked()) {
            throw new WorldLockedException("The world " + getFile() + " has been locked");
        }
        FileUtils.forceDelete(file);
    }

    public void copy(String newName) throws IOException {
        if (!Files.isDirectory(file)) {
            throw new IOException("Not a valid world directory");
        }

        if (isLocked()) {
            throw new WorldLockedException("The world " + getFile() + " has been locked");
        }

        Path newPath = file.resolveSibling(newName);
        FileUtils.copyDirectory(file, newPath, path -> !path.contains("session.lock"));
        World newWorld = new World(newPath);
        newWorld.rename(newName);
    }

    public FileChannel lock() throws WorldLockedException {
        Path lockFile = getSessionLockFile();
        FileChannel channel = null;
        try {
            channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            channel.write(ByteBuffer.wrap("\u2603".getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
            FileLock fileLock = channel.tryLock();
            if (fileLock != null) {
                return channel;
            } else {
                IOUtils.closeQuietly(channel);
                throw new WorldLockedException("The world " + getFile() + " has been locked");
            }
        } catch (IOException e) {
            IOUtils.closeQuietly(channel);
            throw new WorldLockedException(e);
        }
    }

    public void writeLevelDat(CompoundTag nbt) throws IOException {
        if (!Files.isDirectory(file)) {
            throw new IOException("Not a valid world directory");
        }

        FileUtils.saveSafely(getLevelDatFile(), os -> {
            try (OutputStream gos = new GZIPOutputStream(os)) {
                NBTIO.writeTag(gos, nbt);
            }
        });
    }

    private static CompoundTag parseLevelDat(Path path) throws IOException {
        try (InputStream is = new GZIPInputStream(Files.newInputStream(path))) {
            Tag nbt = NBTIO.readTag(is);
            if (nbt instanceof CompoundTag compoundTag) {
                return compoundTag;
            } else {
                throw new IOException("level.dat malformed");
            }
        }
    }

    private static boolean isLocked(Path sessionLockFile) {
        try (FileChannel fileChannel = FileChannel.open(sessionLockFile, StandardOpenOption.WRITE)) {
            return fileChannel.tryLock() == null;
        } catch (AccessDeniedException | OverlappingFileLockException accessDeniedException) {
            return true;
        } catch (NoSuchFileException noSuchFileException) {
            return false;
        } catch (IOException e) {
            LOG.warning("Failed to open the lock file " + sessionLockFile, e);
            return false;
        }
    }

    public static List<World> getWorlds(Path savesDir) {
        if (Files.exists(savesDir)) {
            try (Stream<Path> stream = Files.list(savesDir)) {
                return stream.flatMap(world -> {
                    try {
                        return Stream.of(new World(world.toAbsolutePath().normalize()));
                    } catch (IOException e) {
                        LOG.warning("Failed to read world " + world, e);
                        return Stream.empty();
                    }
                }).toList();
            } catch (IOException e) {
                LOG.warning("Failed to read saves", e);
            }
        }
        return List.of();
    }

    @Override
    public String toString() {
        return "World" + (isLocked() ? " (Locked) " : "") +
                "{name='" + getWorldName() + "'" +
                ",seed=" + getSeed() +
                "}";
    }

    /**
     * @author Xiaotian
     * @see <a href="https://minecraft.wiki/w/Region_file_format">The Region file format</a>
     */
    public static class WorldParser {

        private static final int SECTOR_SIZE = 4096;
        private static final int HEADER_SIZE = 8192;

        private static final int COMPRESSION_ZLIB = 2;
        private static final int COMPRESSION_LZ4 = 4;

        public final DimensionPath overworld;
        public final DimensionPath the_nether;
        public final DimensionPath the_end;

        private final Map<DimensionPath, Set<Region>> worldCache = new ConcurrentHashMap<>();
        private final Map<ChunkId, Chunk> chunkCache = new ConcurrentHashMap<>();

        public final @NotNull World world;

        public WorldParser(@NotNull World world) {
            LOG.info("Parsing world(%s)[%s]".formatted(world.getGameVersion(), world.getWorldName()));

            this.world = world;

            if (Objects.requireNonNull(world.getGameVersion()).isAtLeast("26.1", "26.1-snapshot-6")) {
                Path vanillaWorldPathRoot = world.getFile().resolve("dimensions/minecraft");

                overworld = DimensionPath.of(
                        Files.exists(vanillaWorldPathRoot.resolve("overworld")) ? vanillaWorldPathRoot.resolve("overworld") : null,
                        "overworld",
                        world
                );
                the_nether = DimensionPath.of(
                        Files.exists(vanillaWorldPathRoot.resolve("the_nether")) ? vanillaWorldPathRoot.resolve("the_nether") : null,
                        "the_nether",
                        world
                );
                the_end = DimensionPath.of(
                        Files.exists(vanillaWorldPathRoot.resolve("the_end")) ? vanillaWorldPathRoot.resolve("the_end") : null,
                        "the_end",
                        world
                );
            } else {
                overworld = DimensionPath.of(world.getFile(), "overworld", world);
                the_nether = DimensionPath.of(
                        Files.exists(world.getFile().resolve("DIM-1")) ? world.getFile().resolve("DIM-1") : null,
                        "the_nether",
                        world
                );
                the_end = DimensionPath.of(
                        Files.exists(world.getFile().resolve("DIM1")) ? world.getFile().resolve("DIM1") : null,
                        "the_end",
                        world
                );
            }
        }

        private void initialRegionParse(@NotNull DimensionPath dimensionPath) throws RuntimeException {
            try (Stream<Path> stream = Files.walk(dimensionPath.getRegionPath())) {
                stream.filter(Files::isRegularFile)
                        .forEach(path -> {
                            String fileName = path.getFileName().toString();
                            if (Region.MCA_FILE_PATTERN.matcher(fileName).matches()) {
                                if (!worldCache.containsKey(dimensionPath)) {
                                    worldCache.put(dimensionPath, new HashSet<>());
                                }
                                worldCache.get(dimensionPath).add(new Region(new Chunk[1024], new RegionFile(dimensionPath, path.getFileName())));
                            }
                        });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void regionParse(@NotNull DimensionPath dimensionPath) {
            if (worldCache.isEmpty()) initialRegionParse(dimensionPath);
            if (worldCache.containsKey(dimensionPath)) {
                worldCache.get(dimensionPath).forEach(region -> {
                    try {
                        parseChunks(region);
                    } catch (Exception ignored) {
                    }
                });
            }
        }

        private static @Nullable ChunkId convertToChunkId(int regionChunkX, int regionChunkZ, @NotNull Region region) {
            Matcher m = Region.REGION_GROUP_PATTERN.matcher(region.regionFile().getRegionPath().getFileName().toString());
            if (m.find()) {
                int regionX = Integer.parseInt(m.group(1));
                int regionZ = Integer.parseInt(m.group(2));

                return new ChunkId(regionX * 32 + regionChunkX, regionZ * 32 + regionChunkZ, region.regionFile().dimensionPath);
            }

            return null;
        }

        public static int getCompressType(byte @NotNull [] head) {
            if (head.length < 5) {
                throw new RuntimeException("Illegal head data");
            }
            return head[4] & 0xFF;
        }

        public void parseChunks(@NotNull Region region) throws Exception {
            byte[] header = Files.readAllBytes(region.regionFile().getRegionPath());
            if (header.length < HEADER_SIZE) {
                throw new RuntimeException("Broken file head.");
            }
            // 读取所有区块的偏移信息并截取出来
            for (int i = 0; i < 1024; i++) {
                int headerOffset = i * 4; // head偏移(指向第一个字节)

                int sectorOffset = ((header[headerOffset] & 0xFF) << 16)
                        | ((header[headerOffset + 1] & 0xFF) << 8)
                        | (header[headerOffset + 2] & 0xFF); // 合成24 bit 无符号整数 (大端序)

                int sectorCount = header[headerOffset + 3] & 0xFF;

                if (sectorOffset == 0 || sectorCount == 0) { // 区块未生成
                    region.chunks()[i] = null;
                    continue;
                }

                int dataOffset = sectorOffset * SECTOR_SIZE;
                int compressionType = header[dataOffset + 4] & 0xFF;
                if (dataOffset + 5 > header.length) {
                    LOG.warning("Region file <%s> has an invalid sector at index: <%d>; sector <%d> is out of bounds".formatted(
                                    region.regionFile().regionFile.getFileName().toString(),
                                    i,
                                    dataOffset
                            )
                    );
                    region.chunks()[i] = null;
                    continue;
                }

                int dataLength = ((header[dataOffset] & 0xFF) << 24)
                        | ((header[dataOffset + 1] & 0xFF) << 16)
                        | ((header[dataOffset + 2] & 0xFF) << 8)
                        | (header[dataOffset + 3] & 0xFF);

                byte[] compressedData;
                if ((compressionType & 0x80) != 0) {
                    // 区块数据在额外文件中
                    ChunkId cid = convertToChunkId(i / 32, i % 32, region);
                    if (cid != null) {
                        compressedData = readFromExternalChunkFile(cid.chunkX(), cid.chunkZ(), region.regionFile().dimensionPath);
                    } else {
                        region.chunks()[i] = null;
                        continue;
                    }
                } else {
                    compressedData = Files.readAllBytes(region.regionFile().getRegionPath());
                    if (dataOffset + 5 + dataLength > compressedData.length) {
                        throw new RuntimeException("Illegal chunk data");
                    }
                }

                Tag chunkNBT = NBTIO.readTag(new ByteArrayInputStream(compressedData));

                region.chunks()[i] = new Chunk(
                        convertToChunkId(i / 32, i % 32, region),
                        chunkNBT,
                        new ArrayList<>(),
                        compressionType,
                        region,
                        this
                );
            }
        }

        public static @Nullable ChunkSection @NotNull [] parseSectionsCore(@NotNull Chunk chunk, @NotNull ListTag sections) {
            DimensionPath dimPath = chunk.id.world;
            ChunkSection[] ChunkSections = new ChunkSection[(dimPath.yMax - dimPath.yMin) / 16];
            for (int i = 0; i < sections.size(); i++) {
                Tag tag = sections.get(i);
                if (tag instanceof CompoundTag sectionTag) {
                    if (sectionTag.get("Y") instanceof ByteTag yTag
                            && sectionTag.get("block_states") instanceof LongArrayTag blockStates
                            && sectionTag.get("palette") instanceof ListTag palette)
                    {
                        List<String> blocks = new ArrayList<>();
                        palette.forEach(
                                blockState -> {
                                    if (blockState instanceof CompoundTag blockStateTag
                                            && blockStateTag.get("Name") instanceof StringTag stringTag) {
                                        blocks.add(stringTag.getValue());
                                    }
                                }
                        );
                        ChunkSections[i] = new ChunkSection(chunk, yTag.getValue(), blocks.toArray(new String[0]), blockStates.getValue());
                    }
                }
            }
            return ChunkSections;
        }

        public static void parseSections(@NotNull Chunk chunk) {
            ChunkSection[] sections = new ChunkSection[0];
            if (chunk.chunkNBT instanceof CompoundTag rootTag) {
                if (rootTag.contains("Level")) {
                    sections = parseSectionsBefore21w43a(chunk, rootTag);
                } else {
                    sections = parseSectionsNotBefore21w43a(chunk, rootTag);
                }
            }
            chunk.chunkSections.clear();
            if (sections != null) {
                chunk.chunkSections.addAll(List.of(sections));
            }
        }

        public static @Nullable ChunkSection @Nullable [] parseSectionsBefore21w43a(@NotNull Chunk chunk, @NotNull CompoundTag chunkNBT) {
            if (chunkNBT.get("Level") instanceof CompoundTag levelTag && levelTag.get("Sections") instanceof ListTag sections) {
                return parseSectionsCore(chunk, sections);
            }
            return null;
        }

        public static @Nullable ChunkSection @Nullable [] parseSectionsNotBefore21w43a(@NotNull Chunk chunk, @NotNull CompoundTag chunkNBT) {
            if (chunkNBT.get("sections") instanceof ListTag sections) {
                return parseSectionsCore(chunk, sections);
            }
            return null;
        }

        public static String parseBlockFromSection(@NotNull ChunkSection chunkSection, int x, int y, int z) {
            int localY = y & 0xF;
            int index = (localY << 8) | (z << 4) | x; // y * 16 * 16 + z * 16 + x

            int paletteIndex = Math.toIntExact(chunkSection.data[index]);
            if (paletteIndex < 0 || paletteIndex >= chunkSection.palette.length) {
                return "minecraft:air";
            }
            return chunkSection.palette[paletteIndex];
        }

        public ChunkSection getHighestNonEmptySection(@NotNull Chunk chunk) {
            for (ChunkSection section : chunk.chunkSections.reversed()) {
                if (section != null && section.data.length > 0 && section.palette.length > 0) {
                    return section;
                }
            }
            return null;
        }

        public String getSectionHighestNonAirBlock(@NotNull Chunk chunk, int x, int z) {
            ChunkSection section = getHighestNonEmptySection(chunk);
            if (section != null) {
                for (int y = section.y * 16 + 15; y >= section.y * 16; y--) {
                    String block = parseBlockFromSection(section, x, y, z);
                    if (isNotAirBlock(block)) {
                        return block;
                    }
                }
                ChunkSection[] filteredSections = (ChunkSection[]) chunk.chunkSections.reversed().stream().filter(
                        chunkSection -> chunkSection.equals(section)
                ).toArray(); // most of the sections will not reach there, so the performance loss caused by this is acceptable
                for (ChunkSection s : filteredSections) {
                    if (s != null && s.data.length > 0 && s.palette.length > 0) {
                        for (int y = s.y * 16 + 15; y >= s.y * 16; y--) {
                            String block = parseBlockFromSection(s, x, y, z);
                            if (isNotAirBlock(block)) {
                                return block;
                            }
                        }
                    }
                }
            }
            return "minecraft:air";
        }

        private static byte[] readFromExternalChunkFile(int chunkX, int chunkZ, DimensionPath dimensionPath) {
            try {
                Path externalPath = dimensionPath.getRegionPath().resolve(Paths.get(String.format("c.%d.%d.mcc", chunkX, chunkZ)));

                if (!Files.exists(externalPath)) {
                    throw new RuntimeException("External region file not found.");
                }

                byte[] externalData = Files.readAllBytes(externalPath);
                return decompressData(removeHeader(externalData), getCompressType(externalData));

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private static byte[] decompressData(byte[] data, int compressionType) throws Exception {
            return switch (compressionType) {
                case COMPRESSION_ZLIB -> decompressZlib(data);
                case 3 -> data;
                default -> throw new UnsupportedOperationException("Unsupported compression: " + compressionType);
            };
        }

        private static byte @NotNull [] decompressZlib(byte[] data) throws Exception {
            Inflater inflater = new Inflater();
            inflater.setInput(data);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length * 2);
            byte[] buffer = new byte[1024];

            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }

            outputStream.close();
            return outputStream.toByteArray();
        }

        private static byte @NotNull [] removeHeader(byte @NotNull [] data) {
            return Arrays.copyOfRange(data, 4, data.length);
        }

        private boolean isNotAirBlock(String blockName) {
            return !"minecraft:air".equals(blockName) &&
                    !blockName.startsWith("minecraft:cave_air") &&
                    !blockName.startsWith("minecraft:void_air") &&
                    !blockName.isEmpty();
        }

        public @Nullable Chunk getChunk(@NotNull ChunkId id) {
            return chunkCache.getOrDefault(id, null);
        }

        public @NotNull Chunk getOrParseChunk(@NotNull ChunkId id) {
            if (!chunkCache.containsKey(id)) {
                regionParse(id.world);
                if (!chunkCache.containsKey(id)) {
                    throw new RuntimeException("Chunk not found after parsing region: " + id);
                }
            }
            return chunkCache.get(id);
        }

        public DimensionPath getOverworld() {
            return overworld;
        }

        public DimensionPath getThe_nether() {
            return the_nether;
        }

        public DimensionPath getThe_end() {
            return the_end;
        }

        @Override
        public String toString() {
            return world.toString();
        }

        public record ChunkId(int chunkX, int chunkZ, DimensionPath world) {
        }

        public record Chunk(ChunkId id, Tag chunkNBT, List<ChunkSection> chunkSections, int compressionType, Region region) {
            public Chunk(ChunkId id, Tag chunkNBT, List<ChunkSection> chunkSections, int compressionType, Region region, WorldParser worldParser) {
                this(id, chunkNBT, chunkSections, compressionType, region);
                worldParser.chunkCache.put(id, this);
            }
        }

        public record ChunkSection(Chunk chunkParent, byte y, String[] palette, long[] data) {
        }

        public record Region(Chunk[] chunks, RegionFile regionFile) {
            public static final Pattern MCA_FILE_PATTERN = Pattern.compile("^r\\.-?\\d+\\.-?\\d+\\.mca$");
            public static final Pattern REGION_GROUP_PATTERN = Pattern.compile("^r\\.(?<regionX>-?\\d+)\\.(?<regionZ>-?\\d+)\\.mca$");
        }

        public record DimensionPath(Path dimensionsPath, String worldType, World world, int yMax, int yMin) {

            public static @NotNull DimensionPath of(Path dimensionsPath, String worldType, World world) {
                int yMax = (world.getGameVersion() != null && world.getGameVersion().isAtLeast("1.18", "22w14a")) ? 320 : 256;
                int yMin = (world.getGameVersion() != null && world.getGameVersion().isAtLeast("1.18", "22w14a")) ? -64 : 0;
                return new DimensionPath(dimensionsPath, worldType, world, yMax, yMin);
            }

            public Path getRegionPath() {
                return dimensionsPath.resolve("region");
            }

            @Override
            public @NotNull String toString() {
                return String.format("DimensionPath<%s>{%s}", worldType, dimensionsPath);
            }
        }

        public record RegionFile(DimensionPath dimensionPath, Path regionFile) {
            public @NotNull Path getRegionPath() {
                return dimensionPath.getRegionPath().resolve(regionFile);
            }
        }
    }
}
