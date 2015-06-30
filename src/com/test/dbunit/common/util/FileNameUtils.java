package com.test.dbunit.common.util;

import java.io.File;

import org.apache.commons.io.FilenameUtils;

enum FileNameUtils {
	;

	static boolean isExtensionIgnoreCase(File file, String extension) {
		if (FilenameUtils.getExtension(file.getName()) == null)
			return extension == null;
		return FilenameUtils.getExtension(file.getName()).equalsIgnoreCase(extension);
	}

	static void changeExtension(File file, String extension) {
		String fileName = FilenameUtils.removeExtension(file.getName()) + "." + extension;
		file.renameTo(new File(fileName));
	}

}
