package org.example.service;


import org.example.entity.OrganizationTag;
import org.example.entity.User;
import org.example.exception.CustomException;
import org.example.repository.OrganizationTagRepository;
import org.example.repository.UserRepository;
import org.example.utils.PasswordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class UserService {

    private static final Logger logger = LoggerFactory.getLogger(UserService.class);

    private static final String DEFAULT_ORG_TAG = "DEFAULT";
    private static final String DEFAULT_ORG_NAME = "默认组织";
    private static final String DEFAULT_ORG_DESCRIPTION = "系统默认组织标签，自动分配给所有新用户";
    private static final String PRIVATE_TAG_PREFIX = "PRIVATE_";
    private static final String PRIVATE_ORG_NAME_SUFFIX = "的私人空间";
    private static final String PRIVATE_ORG_DESCRIPTION = "用户的私人组织标签，仅用户本人可访问";

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private OrganizationTagRepository organizationTagRepository;

    @Autowired
    private OrgTagCacheService orgTagCacheService;

    /**
     * 注册新用户。
     *
     * @param username 要注册的用户名
     * @param password 要注册的用户密码
     * @throws CustomException 如果用户名已存在，则抛出异常
     */
    @Transactional
    public void registerUser(String username, String password) {
        // 检查数据库中是否已存在该用户名
        if (userRepository.findByUsername(username).isPresent()) {
            // 若用户名已存在，抛出自定义异常，状态码为 400 Bad Request
            throw new CustomException("Username already exists", HttpStatus.BAD_REQUEST);
        }

        // 确保默认组织标签存在（系统内部使用）
        ensureDefaultOrgTagExists();

        User user = new User();
        user.setUsername(username);
        // 对密码进行加密处理并设置到 User 对象中
        user.setPassword(PasswordUtils.encode(password));
        // 设置用户角色为普通用户
        user.setRole(User.Role.USER);

        // 保存用户以生成ID
        userRepository.save(user);

        // 创建用户的私人组织标签
        String privateTagId = PRIVATE_TAG_PREFIX + username;
        createPrivateOrgTag(privateTagId, username, user);

        // 只分配私人组织标签
        user.setOrgTags(privateTagId);

        // 设置私人组织标签为主组织标签
        user.setPrimaryOrg(privateTagId);

        userRepository.save(user);

        // 缓存组织标签信息
        orgTagCacheService.cacheUserOrgTags(username, List.of(privateTagId));
        orgTagCacheService.cacheUserPrimaryOrg(username, privateTagId);

        logger.info("User registered successfully with private organization tag: {}", username);
    }


    /**
     * 确保默认组织标签存在
     */
    private void ensureDefaultOrgTagExists() {
        if (!organizationTagRepository.existsByTagId(DEFAULT_ORG_TAG)) {
            logger.info("Creating default organization tag");

            // 寻找一个管理员用户作为创建者
            Optional<User> adminUser = userRepository.findAll().stream()
                    .filter(user -> User.Role.ADMIN.equals(user.getRole()))
                    .findFirst();

            User creator;
            if (adminUser.isPresent()) {
                creator = adminUser.get();
            } else {
                // 如果没有管理员用户，则创建一个系统用户作为创建者
                creator = createSystemAdminIfNotExists();
            }

            // 创建默认组织标签
            OrganizationTag defaultTag = new OrganizationTag();
            defaultTag.setTagId(DEFAULT_ORG_TAG);
            defaultTag.setName(DEFAULT_ORG_NAME);
            defaultTag.setDescription(DEFAULT_ORG_DESCRIPTION);
            defaultTag.setCreatedBy(creator);

            organizationTagRepository.save(defaultTag);
            logger.info("Default organization tag created successfully");
        }
    }

    /**
     * 创建用户的私人组织标签
     */
    private void createPrivateOrgTag(String privateTagId, String username, User owner) {
        // 检查私人标签是否已存在
        if (!organizationTagRepository.existsByTagId(privateTagId)) {
            logger.info("Creating private organization tag for user: {}", username);

            // 创建私人组织标签
            OrganizationTag privateTag = new OrganizationTag();
            privateTag.setTagId(privateTagId);
            privateTag.setName(username + PRIVATE_ORG_NAME_SUFFIX);
            privateTag.setDescription(PRIVATE_ORG_DESCRIPTION);
            privateTag.setCreatedBy(owner);

            organizationTagRepository.save(privateTag);
            logger.info("Private organization tag created successfully for user: {}", username);
        }
    }

    /**
     * 如果系统中没有管理员用户，则创建一个系统管理员
     */
    private User createSystemAdminIfNotExists() {
        String systemAdminUsername = "system_admin";

        return userRepository.findByUsername(systemAdminUsername)
                .orElseGet(() -> {
                    logger.info("Creating system admin user");
                    User systemAdmin = new User();
                    systemAdmin.setUsername(systemAdminUsername);
                    // 生成随机密码
                    String randomPassword = generateRandomPassword();
                    systemAdmin.setPassword(PasswordUtils.encode(randomPassword));
                    systemAdmin.setRole(User.Role.ADMIN);

                    logger.info("System admin created with password: {}", randomPassword);
                    return userRepository.save(systemAdmin);
                });
    }

    /**
     * 生成随机密码
     */
    private String generateRandomPassword() {
        // 生成16位随机密码
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 16; i++) {
            int index = (int) (Math.random() * chars.length());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }

    // ------------------- 用户登录方法 ------------------
    public String authenticateUser(String username, String password) {
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new CustomException("Invalid username or password", HttpStatus.UNAUTHORIZED));
        // 比较输入的密码和数据库中存储的加密密码是否匹配
        if (!PasswordUtils.matches(password, user.getPassword())) {
            // 若不匹配，抛出自定义异常，状态码为 401 Unauthorized
            throw new CustomException("Invalid username or password", HttpStatus.UNAUTHORIZED);
        }
        // 认证成功，返回用户的用户名
        return user.getUsername();
    }

    @Transactional
    public OrganizationTag createOrganizationTag(String tagId, String name, String description,
                                                 String parentTag, String creatorUsername) {
        // 验证创建者是否为管理员
        User creator = userRepository.findByUsername(creatorUsername)
                .orElseThrow(() -> new CustomException("Creator not found", HttpStatus.NOT_FOUND));

        if (creator.getRole() != User.Role.ADMIN) {
            throw new CustomException("Only administrators can create organization tags", HttpStatus.FORBIDDEN);
        }

        // 检查标签ID是否已存在
        if (organizationTagRepository.existsByTagId(tagId)) {
            throw new CustomException("Tag ID already exists", HttpStatus.BAD_REQUEST);
        }

        // 如果指定了父标签，检查父标签是否存在
        if (parentTag != null && !parentTag.isEmpty()) {
            organizationTagRepository.findByTagId(parentTag)
                    .orElseThrow(() -> new CustomException("Parent tag not found", HttpStatus.NOT_FOUND));
        }

        OrganizationTag tag = new OrganizationTag();
        tag.setTagId(tagId);
        tag.setName(name);
        tag.setDescription(description);
        tag.setParentTag(parentTag);
        tag.setCreatedBy(creator);

        OrganizationTag savedTag = organizationTagRepository.save(tag);

        // 清除标签缓存，因为层级关系可能变化
        //orgTagCacheService.invalidateAllEffectiveTagsCache();

        return savedTag;
    }

    /**
     * 为用户分配组织标签
     *
     * @param userId 用户ID
     * @param orgTags 组织标签ID列表
     * @param adminUsername 管理员用户名
     */
    @Transactional
    public void assignOrgTagsToUser(Long userId, List<String> orgTags, String adminUsername) {
        // 验证操作者是否为管理员
        User admin = userRepository.findByUsername(adminUsername)
                .orElseThrow(() -> new CustomException("Admin not found", HttpStatus.NOT_FOUND));

        if (admin.getRole() != User.Role.ADMIN) {
            throw new CustomException("Only administrators can assign organization tags", HttpStatus.FORBIDDEN);
        }

        // 查找用户
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new CustomException("User not found", HttpStatus.NOT_FOUND));

        // 验证所有标签是否存在
        for (String tagId : orgTags) {
            if (!organizationTagRepository.existsByTagId(tagId)) {
                throw new CustomException("Organization tag " + tagId + " not found", HttpStatus.NOT_FOUND);
            }
        }

        // 获取用户的现有组织标签
        Set<String> existingTags = new HashSet<>();
        if (user.getOrgTags() != null && !user.getOrgTags().isEmpty()) {
            existingTags = Arrays.stream(user.getOrgTags().split(",")).collect(Collectors.toSet());
        }

        // 找出并保留用户的私人组织标签
        String privateTagId = PRIVATE_TAG_PREFIX + user.getUsername();
        boolean hasPrivateTag = existingTags.contains(privateTagId);

        // 确保用户的私人组织标签不会被删除
        Set<String> finalTags = new HashSet<>(orgTags);
        if (hasPrivateTag && !finalTags.contains(privateTagId)) {
            finalTags.add(privateTagId);
        }

        // 将标签列表转换为逗号分隔的字符串
        String orgTagsStr = String.join(",", finalTags);
        user.setOrgTags(orgTagsStr);

        // 如果用户没有主组织标签且有组织标签，则优先使用私人标签作为主组织
        if ((user.getPrimaryOrg() == null || user.getPrimaryOrg().isEmpty()) && !finalTags.isEmpty()) {
            if (hasPrivateTag) {
                user.setPrimaryOrg(privateTagId);
            } else {
                user.setPrimaryOrg(new ArrayList<>(finalTags).get(0));
            }
        }

        userRepository.save(user);

        // 更新缓存
        orgTagCacheService.deleteUserOrgTagsCache(user.getUsername());
        orgTagCacheService.cacheUserOrgTags(user.getUsername(), new ArrayList<>(finalTags));
        // 同时清除有效标签缓存
        orgTagCacheService.deleteUserEffectiveTagsCache(user.getUsername());

        if (user.getPrimaryOrg() != null && !user.getPrimaryOrg().isEmpty()) {
            orgTagCacheService.cacheUserPrimaryOrg(user.getUsername(), user.getPrimaryOrg());
        }
    }

    /**
     * 获取用户的组织标签信息
     *
     * @param username 用户名
     * @return 包含用户组织标签信息的Map
     */
    public Map<String, Object> getUserOrgTags(String username) {
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new CustomException("User not found", HttpStatus.NOT_FOUND));

        // 尝试从缓存获取
        List<String> orgTags = orgTagCacheService.getUserOrgTags(username);
        String primaryOrg = orgTagCacheService.getUserPrimaryOrg(username);

        // 如果缓存中没有，则从数据库获取
        if (orgTags == null || orgTags.isEmpty()) {
            orgTags = Arrays.asList(user.getOrgTags().split(","));
            // 更新缓存
            orgTagCacheService.cacheUserOrgTags(username, orgTags);
        }

        if (primaryOrg == null || primaryOrg.isEmpty()) {
            primaryOrg = user.getPrimaryOrg();
            // 更新缓存
            orgTagCacheService.cacheUserPrimaryOrg(username, primaryOrg);
        }

        // 获取组织标签的详细信息
        List<Map<String, String>> orgTagDetails = new ArrayList<>();
        for (String tagId : orgTags) {
            OrganizationTag tag = organizationTagRepository.findByTagId(tagId)
                    .orElse(null);
            if (tag != null) {
                Map<String, String> tagInfo = new HashMap<>();
                tagInfo.put("tagId", tag.getTagId());
                tagInfo.put("name", tag.getName());
                tagInfo.put("description", tag.getDescription());
                orgTagDetails.add(tagInfo);
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("orgTags", orgTags);
        result.put("primaryOrg", primaryOrg);
        result.put("orgTagDetails", orgTagDetails);

        return result;
    }

    /**
     * 设置用户的主组织标签
     *
     * @param username 用户名
     * @param primaryOrg 主组织标签
     */
    public void setUserPrimaryOrg(String username, String primaryOrg) {
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new CustomException("User not found", HttpStatus.NOT_FOUND));

        // 检查该组织标签是否已分配给用户
        Set<String> userTags = Arrays.stream(user.getOrgTags().split(",")).collect(Collectors.toSet());
        if (!userTags.contains(primaryOrg)) {
            throw new CustomException("Organization tag not assigned to user", HttpStatus.BAD_REQUEST);
        }

        user.setPrimaryOrg(primaryOrg);
        userRepository.save(user);

        // 更新缓存
        orgTagCacheService.cacheUserPrimaryOrg(username, primaryOrg);
    }

    /**
     * 获取用户的主组织标签
     *
     * @param userId 用户ID
     * @return 用户的主组织标签
     */
    public String getUserPrimaryOrg(String userId) {
        // 先通过userId查找用户，然后获取username
        User user;
        try {
            Long userIdLong = Long.parseLong(userId);
            user = userRepository.findById(userIdLong)
                    .orElseThrow(() -> new CustomException("User not found with ID: " + userId, HttpStatus.NOT_FOUND));
        } catch (NumberFormatException e) {
            // 如果userId不是数字格式，则假设它就是username
            user = userRepository.findByUsername(userId)
                    .orElseThrow(() -> new CustomException("User not found: " + userId, HttpStatus.NOT_FOUND));
        }

        String username = user.getUsername();

        // 尝试从缓存获取
        String primaryOrg = orgTagCacheService.getUserPrimaryOrg(username);

        // 如果缓存中没有，则从数据库获取
        if (primaryOrg == null || primaryOrg.isEmpty()) {
            primaryOrg = user.getPrimaryOrg();

            // 如果用户没有设置主组织标签，则尝试使用第一个分配的组织标签
            if (primaryOrg == null || primaryOrg.isEmpty()) {
                String[] tags = user.getOrgTags().split(",");
                if (tags.length > 0) {
                    primaryOrg = tags[0];
                    // 更新用户的主组织标签
                    user.setPrimaryOrg(primaryOrg);
                    userRepository.save(user);
                } else {
                    // 如果用户没有任何组织标签，则使用默认标签
                    primaryOrg = DEFAULT_ORG_TAG;
                }
            }

            // 更新缓存
            orgTagCacheService.cacheUserPrimaryOrg(username, primaryOrg);
        }

        return primaryOrg;
    }
}